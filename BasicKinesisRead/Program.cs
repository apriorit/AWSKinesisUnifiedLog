using Amazon.Kinesis;
using Amazon.Kinesis.Model;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BasicKinesisRead
{
    class Program
    {
        private const string _streamName = "events";
        static async Task Main()
        {
            var cancelationSource = new CancellationTokenSource();
            var shardReader = new ShardReader();
            IEnumerable<Shard> shards = await shardReader.GetShards(_streamName);
            List<Thread> eventThreads = new List<Thread>();
            foreach (var shard in shards)
            {
                var eventThread = new Thread(async () => await shardReader.ProcessEvents(shard, _streamName, cancelationSource.Token));
                eventThread.Start();
                eventThreads.Add(eventThread);
            }
            Console.ReadLine();
            cancelationSource.Cancel();
            eventThreads.ForEach(eventThread => eventThread.Join());
        }
    }
}
