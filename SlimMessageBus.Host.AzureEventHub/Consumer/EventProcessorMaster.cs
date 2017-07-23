using System;
using System.Collections.Generic;
using Common.Logging;
using Microsoft.ServiceBus.Messaging;
using SlimMessageBus.Host.Config;

namespace SlimMessageBus.Host.AzureEventHub
{
    public class EventProcessorMaster : IDisposable, IEventProcessorFactory
    {
        private static readonly ILog Log = LogManager.GetLogger<EventProcessorMaster>();

        public readonly EventHubMessageBus MessageBus;

        protected readonly EventProcessorHost ProcessorHost;
        protected readonly List<EventProcessor> Processors = new List<EventProcessor>();
        protected readonly Func<EventProcessorMaster, EventProcessor> ProcessorFactory;

        public bool CanRun { get; protected set; } = true;

        public EventProcessorMaster(EventHubMessageBus messageBus, ConsumerSettings consumerSettings)
            : this(messageBus, consumerSettings, x => new EventProcessorForConsumers(x, consumerSettings))
        {
        }

        public EventProcessorMaster(EventHubMessageBus messageBus, RequestResponseSettings requestResponseSettings)
            : this(messageBus, requestResponseSettings, x => new EventProcessorForResponses(x, requestResponseSettings))
        {
        }

        protected EventProcessorMaster(EventHubMessageBus messageBus, ITopicGroupConsumerSettings consumerSettings, Func<EventProcessorMaster, EventProcessor> processorFactory)
        {
            MessageBus = messageBus;
            ProcessorFactory = processorFactory;

            Log.InfoFormat("Creating EventProcessorHost for Topic: {0}, Group: {1}", consumerSettings.Topic, consumerSettings.Group);
            ProcessorHost = MessageBus.EventHubSettings.EventProcessorHostFactory(consumerSettings);
            var eventProcessorOptions = MessageBus.EventHubSettings.EventProcessorOptionsFactory(consumerSettings);
            ProcessorHost.RegisterEventProcessorFactoryAsync(this, eventProcessorOptions).Wait();
        }

        #region Implementation of IDisposable

        public void Dispose()
        {
            if (!CanRun)
            {
                return;
            }

            CanRun = false;

            ProcessorHost.UnregisterEventProcessorAsync().Wait();
            ProcessorHost.Dispose();

            if (Processors.Count > 0)
            {
                Processors.ForEach(ep => ep.DisposeSilently("EventProcessor", Log));
                Processors.Clear();
            }
        }

        #endregion

        #region Implementation of IEventProcessorFactory

        public IEventProcessor CreateEventProcessor(PartitionContext context)
        {
            Log.DebugFormat("Creating EventHubEventProcessor for {0}", new PartitionContextInfo(context));
            var ep = ProcessorFactory(this);
            Processors.Add(ep);
            return ep;
        }

        #endregion
    }
}