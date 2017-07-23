using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using Confluent.Kafka;

namespace SlimMessageBus.Host.Kafka
{
    public class KafkaGroupConsumer : IDisposable, IKafkaCommitController
    {
        private static readonly ILog Log = LogManager.GetLogger<KafkaGroupConsumer>();

        public KafkaMessageBus MessageBus { get; }
        public string Group { get; }
        public IEnumerable<string> Topics => _processorByTopic.Keys;

        private readonly IDictionary<string, IKafkaTopicProcessor> _processorByTopic;
        private Consumer _consumer;

        private Task _consumerTask;
        private CancellationTokenSource _consumerCts;

        public KafkaGroupConsumer(KafkaMessageBus messageBus, string group, IEnumerable<IKafkaTopicProcessor> processors)
        {
            MessageBus = messageBus;
            Group = group;

            _processorByTopic = new Dictionary<string, IKafkaTopicProcessor>();
            foreach (var processor in processors)
            {
                processor.CommitController = this;
                _processorByTopic.Add(processor.Topic, processor);
            }

            Log.InfoFormat("Creating consumer for group: {0}", group);
            _consumer = CreateConsumer(group);
            _consumer.OnMessage += OnMessage;
            _consumer.OnPartitionsAssigned += OnPartitionAssigned;
            _consumer.OnPartitionsRevoked += OnPartitionRevoked;
            _consumer.OnPartitionEOF += OnPartitionEndReached;
            _consumer.OnOffsetsCommitted += OnOffsetsCommitted;
            _consumer.OnStatistics += OnStatistics;
        }

        #region Implementation of IDisposable

        public virtual void Dispose()
        {
            if (_consumerTask != null)
            {
                Stop();
            }

            if (_processorByTopic.Count > 0)
            {
                foreach (var topicProcessor in _processorByTopic.Values)
                {
                    topicProcessor.Dispose();                    
                }
                _processorByTopic.Clear();                
            }

            // dispose the consumer
            if (_consumer != null)
            {
                _consumer.DisposeSilently("consumer", Log);
                _consumer = null;
            }
        }

        #endregion

        protected Consumer CreateConsumer(string group)
        {
            var config = MessageBus.KafkaSettings.ConsumerConfigFactory(group);
            config[KafkaConfigKeys.Servers] = MessageBus.KafkaSettings.BrokerList;
            config[KafkaConfigKeys.Consumer.GroupId] = group;
            // ToDo: add support for auto commit
            config[KafkaConfigKeys.Consumer.EnableAutoCommit] = false;
            var consumer = MessageBus.KafkaSettings.ConsumerFactory(group, config);
            return consumer;
        }

        protected virtual void OnOffsetsCommitted(object sender, CommittedOffsets e)
        {
            if (e.Error)
            {
                if (Log.IsWarnEnabled)
                    Log.WarnFormat("Failed to commit offsets: [{0}], error: {1}", string.Join(", ", e.Offsets), e.Error);
            }
            else
            {
                if (Log.IsTraceEnabled)
                    Log.TraceFormat("Successfully committed offsets: [{0}]", string.Join(", ", e.Offsets));
            }
        }

        protected virtual void OnStatistics(object sender, string e)
        {
            if (Log.IsTraceEnabled)
            {
                Log.TraceFormat("Statistics: {0}", e);
            }
        }

        public void Start()
        {
            if (_consumerTask != null)
            {
                throw new MessageBusException($"Consumer for group {Group} already started");
            }

            if (Log.IsInfoEnabled)
            {
                Log.InfoFormat("Subscribing to topics: {0}", string.Join(",", Topics));
            }
            _consumer.Subscribe(Topics);

            _consumerCts = new CancellationTokenSource();
            var ct = _consumerCts.Token;
            var ts = TimeSpan.FromSeconds(2);
            _consumerTask = Task.Factory.StartNew(() =>
            {
                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        Log.Debug("Polling consumer");
                        try
                        {
                            _consumer.Poll(ts);
                        }
                        catch (Exception e)
                        {
                            Log.ErrorFormat("Group [{0}]: Error occured while polling new messages (will retry)", e, Group);
                        }
                    }
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Group [{0}]: Error occured in group loop (terminated)", e, Group);
                }
            }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public void Stop()
        {
            if (_consumerTask == null)
            {
                throw new MessageBusException($"Consumer for group {Group} not yet started");
            }

            Log.Info("Unassigning partitions");
            _consumer.Unassign();

            Log.Info("Unsubscribing from topics");
            _consumer.Unsubscribe();

            _consumerCts.Cancel();
            try
            {
                _consumerTask.Wait();
            }
            finally
            {
                _consumerTask = null;
                _consumerCts = null;
            }
        }

        protected virtual void OnPartitionAssigned(object sender, List<TopicPartition> partitions)
        {
            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Group [{0}]: Assigned partitions: {1}", Group, string.Join(", ", partitions));
            }
            _consumer?.Assign(partitions);
        }

        protected virtual void OnPartitionRevoked(object sender, List<TopicPartition> partitions)
        {
            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Group [{0}]: Revoked partitions: {1}", Group, string.Join(", ", partitions));
            }
            _consumer?.Unassign();
        }

        protected virtual void OnPartitionEndReached(object sender, TopicPartitionOffset offset)
        {
            Log.DebugFormat("Group [{0}]: Reached end of topic: {1} and partition: {2}, next message will be at offset: {3}", Group, offset.Topic, offset.Partition, offset.Offset);

            var topicProcessor = _processorByTopic[offset.Topic];
            topicProcessor.OnPartitionEndReached(offset).Wait();
        }

        protected virtual void OnMessage(object sender, Message message)
        {
            Log.DebugFormat("Group [{0}]: Received message on topic: {1} (offset: {2}, payload size: {3})", Group, message.Topic, message.TopicPartitionOffset, message.Value.Length);

            var topicProcessor = _processorByTopic[message.Topic];
            topicProcessor.OnMessage(message).Wait();
        }

        #region Implementation of IKafkaCoordinator

        public Task Commit(TopicPartitionOffset offset)
        {
            return _consumer.CommitAsync(new List<TopicPartitionOffset> { offset });
        }    

        #endregion
    }
}