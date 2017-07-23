using System;
using System.Threading.Tasks;
using Common.Logging;
using Confluent.Kafka;
using SlimMessageBus.Host.Config;

namespace SlimMessageBus.Host.Kafka
{
    public class KafkaResponseConsumer : IKafkaTopicProcessor
    {
        private static readonly ILog Log = LogManager.GetLogger<KafkaResponseConsumer>();

        private readonly RequestResponseSettings _requestResponseSettings;
        private readonly MessageBusBase _messageBus;
        private readonly ICheckpointTrigger _checkpointTrigger;

        public KafkaResponseConsumer(RequestResponseSettings requestResponseSettings, MessageBusBase messageBus)
        {
            _requestResponseSettings = requestResponseSettings;
            _messageBus = messageBus;
            _checkpointTrigger = new CheckpointTrigger(requestResponseSettings);
        }

        #region Implementation of IDisposable

        public void Dispose()
        {
        }

        #endregion

        #region Implementation of IKafkaTopicProcessor

        public string Topic => _requestResponseSettings.Topic;
        public IKafkaCommitController CommitController { get; set; }

        public async Task OnMessage(Message message)
        {
            try
            {
                _messageBus.OnResponseArrived(message.Value, _requestResponseSettings.Topic).Wait();
            }
            catch (Exception e)
            {
                if (Log.IsErrorEnabled)
                {
                    Log.ErrorFormat("Error occured while consuming response message, {0}", e, new MessageContextInfo(_requestResponseSettings.Group, message));
                }

                // we can only continue and process all messages in the lease

                if (_requestResponseSettings.OnResponseMessageFault != null)
                {
                    // Call the hook
                    Log.DebugFormat("Executing the attached hook from {0}", nameof(_requestResponseSettings.OnResponseMessageFault));
                    _requestResponseSettings.OnResponseMessageFault(_requestResponseSettings, message, e);
                }
            }
            if (_checkpointTrigger.Increment())
            {
                await CommitController.Commit(message.TopicPartitionOffset);
            }
        }

        public async Task OnPartitionEndReached(TopicPartitionOffset offset)
        {
            await CommitController.Commit(offset);
        }

        #endregion
    }
}