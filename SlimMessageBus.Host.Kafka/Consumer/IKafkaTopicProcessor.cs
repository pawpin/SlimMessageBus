using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace SlimMessageBus.Host.Kafka
{
    public interface IKafkaTopicProcessor : IDisposable
    {        
        string Topic { get; }
        IKafkaCommitController CommitController { get; set; }

        Task OnMessage(Message message);
        Task OnPartitionEndReached(TopicPartitionOffset offset);
    }
}