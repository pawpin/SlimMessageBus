using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Logging;
using Microsoft.ServiceBus.Messaging;

namespace SlimMessageBus.Host.AzureEventHub
{
    public abstract class EventProcessor : IEventProcessor, IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger<EventProcessor>();

        protected readonly EventProcessorMaster Master;

        protected EventProcessor(EventProcessorMaster master)
        {
            Master = master;
        }

        #region Implementation of IDisposable

        public abstract void Dispose();

        #endregion

        #region Implementation of IEventProcessor

        public Task OpenAsync(PartitionContext context)
        {
            Log.DebugFormat("Open lease: {0}", new PartitionContextInfo(context));
            return Task.CompletedTask;
        }

        public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            EventData lastMessage = null;
            EventData lastCheckpointMessage = null;
            var skipLastCheckpoint = false;

            foreach (var message in messages)
            {
                if (!Master.CanRun)
                {
                    break;
                }

                lastMessage = message;

                lastCheckpointMessage = await CheckpointSafe(message, context);
                if (!ReferenceEquals(lastCheckpointMessage, message))
                {
                    // something went wrong (not all messages were processed with success)

                    // ToDo: add retry support
                    //skipLastCheckpoint = !ReferenceEquals(lastCheckpointMessage, message);
                    //skipLastCheckpoint = false;
                }
            }

            if (!skipLastCheckpoint)
            {
                // checkpoint the last messages
                if ((!ReferenceEquals(lastCheckpointMessage, lastMessage) && lastMessage != null))
                {
                    await CheckpointSafe(lastMessage, context);
                }
            }
        }

        private async Task<EventData> CheckpointSafe(EventData message, PartitionContext context)
        {
            EventData lastGoodMessage;
            EventData lastCheckpointMessage;

            if (OnCommit(out lastGoodMessage))
            {
                // all messages were successful
                lastCheckpointMessage = message;
            }
            else
            {
                // something went wrong (not all messages were processed with success)

                // checkpoint all the succeeded messages (in order) until the first failed one
                lastCheckpointMessage = lastGoodMessage;

                // call the hook of all the ones that failed
                // ToDo: call the hook
            }
            if (lastCheckpointMessage != null)
            {
                await Checkpoint(lastCheckpointMessage, context);
            }
            return lastCheckpointMessage;
        }

        private async Task Checkpoint(EventData message, PartitionContext context)
        {
            Log.DebugFormat("Will checkpoint at Offset: {0}, {1}", message.Offset, new PartitionContextInfo(context));
            await context.CheckpointAsync(message);
        }

        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            Log.DebugFormat("Close lease: Reason: {0}, {1}", reason, new PartitionContextInfo(context));
            return Task.CompletedTask;
        }

        #endregion

        protected abstract bool OnSubmit(EventData message, PartitionContext context);
        protected abstract bool OnCommit(out EventData lastGoodMessage);
    }
}