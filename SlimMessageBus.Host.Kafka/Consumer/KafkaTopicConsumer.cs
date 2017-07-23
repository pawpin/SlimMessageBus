using System;
using System.Threading.Tasks;
using Common.Logging;
using Confluent.Kafka;
using SlimMessageBus.Host.Config;

namespace SlimMessageBus.Host.Kafka
{
    public class KafkaTopicConsumer : IKafkaTopicProcessor
    {
        private static readonly ILog Log = LogManager.GetLogger<KafkaTopicConsumer>();

        private readonly ConsumerSettings _consumerSettings;
        private readonly ConsumerInstancePool<Message> _consumerInstancePool;
        private readonly MessageQueueWorker<Message> _messageQueueWorker; 

        public KafkaTopicConsumer(ConsumerSettings consumerSettings, MessageBusBase messageBus)
        {
            _consumerSettings = consumerSettings;
            _consumerInstancePool = new ConsumerInstancePool<Message>(consumerSettings, messageBus, m => m.Value);
            _messageQueueWorker = new MessageQueueWorker<Message>(_consumerInstancePool, new CheckpointTrigger(consumerSettings));
        }

        #region IDisposable

        public void Dispose()
        {
            _consumerInstancePool.Dispose();
        }

        #endregion

        #region Implementation of IKafkaTopicProcessor

        public string Topic => _consumerSettings.Topic;

        public IKafkaCommitController CommitController { get; set; }

        public async Task OnMessage(Message message)
        {
            try
            {
                if (_messageQueueWorker.Submit(message))
                {
                    Log.DebugFormat("Will commit at offset {0}", message.TopicPartitionOffset);
                    await Commit(message.TopicPartitionOffset);
                }
            }
            catch (Exception e)
            {
                Log.ErrorFormat("Group [{0}]: Error occured while processing a message from topic {0} of type {1}. {2}", _consumerSettings.Group, message.Topic, _consumerSettings.MessageType, e);
                throw;
            }
        }

        public async Task OnPartitionEndReached(TopicPartitionOffset offset)
        {
            await Commit(offset);
        }

        #endregion

        public async Task Commit(TopicPartitionOffset offset)
        {
            Message lastGoodMessage;
            _messageQueueWorker.Commit(out lastGoodMessage);
            //lastGoodMessage.TopicPartitionOffset != offset
            await CommitController.Commit(offset);
        }

    }
}