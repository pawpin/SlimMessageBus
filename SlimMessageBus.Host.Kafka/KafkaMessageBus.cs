using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Logging;
using Confluent.Kafka;
using SlimMessageBus.Host.Config;

namespace SlimMessageBus.Host.Kafka
{
    /// <summary>
    ///
    /// Note the assumption is that Topic/Producer/Consumer are all thread-safe (see https://github.com/edenhill/librdkafka/issues/215)
    /// </summary>
    public class KafkaMessageBus : MessageBusBase
    {
        private static readonly ILog Log = LogManager.GetLogger<KafkaMessageBus>();

        public KafkaMessageBusSettings KafkaSettings { get; }

        private Producer _producer;
        private readonly IList<KafkaGroupConsumer> _groupConsumers = new List<KafkaGroupConsumer>();

        public Producer CreateProducer()
        {
            Log.Debug("Creating producer settings");
            var config = KafkaSettings.ProducerConfigFactory();
            config[KafkaConfigKeys.Servers] = KafkaSettings.BrokerList;
            Log.DebugFormat("Producer settings: {0}", config);
            var producer = KafkaSettings.ProducerFactory(config);
            return producer;
        }

        public KafkaMessageBus(MessageBusSettings settings, KafkaMessageBusSettings kafkaSettings)
            : base(settings)
        {
            AssertSettings(settings);

            KafkaSettings = kafkaSettings;

            Log.Info("Creating producer");
            _producer = CreateProducer();
            Log.InfoFormat("Producer has been assigned name: {0}", _producer.Name);

            Log.Info("Creating consumers");
            foreach (var consumersByGroup in settings.Consumers.GroupBy(x => x.Group))
            {
                var group = consumersByGroup.Key;

                var topicProcessors = consumersByGroup.Select(cs =>
                {
                    Log.InfoFormat("Creating {0} for Topic: {1}, Group: {2}, MessageType: {3}", nameof(KafkaTopicConsumer), cs.Topic, cs.Group, cs.MessageType);
                    return new KafkaTopicConsumer(cs, this);
                }).ToList();

                AddGroupConsumer(settings, group, topicProcessors);
            }

            if (settings.RequestResponse != null)
            {
                Log.InfoFormat("Creating {0} for Topic: {1} and Group: {2}", nameof(KafkaResponseConsumer), settings.RequestResponse.Group, settings.RequestResponse.Topic);
                var responseProcessor = new KafkaResponseConsumer(settings.RequestResponse, this);

                AddGroupConsumer(settings, settings.RequestResponse.Group, new[] { responseProcessor });
            }

            Start();
        }

        private void Start()
        {
            Log.Info("Starting group consumers...");
            foreach (var groupConsumer in _groupConsumers)
            {
                groupConsumer.Start();
            }
            Log.Info("Group consumers started");
        }

        private void AddGroupConsumer(MessageBusSettings settings, string group, IEnumerable<IKafkaTopicProcessor> processors)
        {
            Log.InfoFormat("Creating {0} for Group: {1}", group, nameof(KafkaGroupConsumer));
            _groupConsumers.Add(new KafkaGroupConsumer(this, group, processors));
        }

        private static void AssertSettings(MessageBusSettings settings)
        {
            if (settings.RequestResponse != null)
            {
                Assert.IsTrue(settings.RequestResponse.Group != null,
                    () => new InvalidConfigurationMessageBusException($"Request-response: group was not provided"));

                Assert.IsTrue(settings.Consumers.All(x => x.Group != settings.RequestResponse.Group),
                    () => new InvalidConfigurationMessageBusException($"Request-response: group cannot be shared with consumer groups"));
            }
        }

        #region Overrides of BaseMessageBus

        protected override void OnDispose()
        {
            foreach (var groupConsumer in _groupConsumers)
            {
                groupConsumer.DisposeSilently(() => $"consumer group {groupConsumer.Group}", Log);
            }
            _groupConsumers.Clear();

            if (_producer != null)
            {
                _producer.DisposeSilently("producer", Log);
                _producer = null;
            }

            base.OnDispose();
        }

        public override async Task Publish(Type messageType, byte[] payload, string topic)
        {
            AssertActive();

            Log.DebugFormat("Producing message of type {0} on topic {1} with size {2}", messageType.Name, topic, payload.Length);
            // send the message to topic
            var deliveryReport = await _producer.ProduceAsync(topic, null, payload);
            // log some debug information
            Log.DebugFormat("Delivered message at {0}", deliveryReport.TopicPartitionOffset);
        }

        #endregion
    }
}