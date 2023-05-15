using Amazon.Kinesis;
using Amazon.Kinesis.Model;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace BasicKinesisRead
{
    class ShardReader
    {
        AmazonKinesisClient _kinesisClient = new AmazonKinesisClient(new AmazonKinesisConfig());

        public async Task<IEnumerable<Shard>> GetShards(string streamName)
        {
            var shards = new List<Shard>();
            ListShardsResponse response = null;
            do
            {
                var request = response == null ? new ListShardsRequest { StreamName = streamName }
                : new ListShardsRequest { NextToken = response.NextToken };
                response = await _kinesisClient.ListShardsAsync(request);
                shards.AddRange(response.Shards);
            }
            while (response.NextToken != null);
            return shards;
        }

        public async Task ProcessEvents(Shard shard, string streamName, CancellationToken cancellationToken)
        {
            var iteratorRequest = new GetShardIteratorRequest()
            {
                ShardId = shard.ShardId,
                ShardIteratorType = ShardIteratorType.TRIM_HORIZON,
                StreamName = streamName

            };
            string shardIterator = (await _kinesisClient.GetShardIteratorAsync(iteratorRequest)).ShardIterator;

            do
            {
                try
                {
                    var recordsRequest = new GetRecordsRequest
                    {
                        ShardIterator = shardIterator,
                        Limit = 10
                    };
                    GetRecordsResponse recordsResponse = await _kinesisClient.GetRecordsAsync(recordsRequest);
                    await ProcessRecords(recordsResponse.Records);
                    shardIterator = recordsResponse.NextShardIterator;
                }
                catch(ProvisionedThroughputExceededException)
                {
                    await Task.Delay(1000);
                }
            }
            while (!cancellationToken.IsCancellationRequested && shardIterator != null);
        }

        private async Task ProcessRecords(List<Record> records)
        {
            foreach(var record in records)
            {
                var diskEvent = await JsonSerializer.DeserializeAsync<Event>(record.Data);
                long availableDiskPercent = diskEvent.AvailableSpace * 100 / diskEvent.TotalSize;
                if (availableDiskPercent < 20)
                {
                    Console.WriteLine($"{diskEvent.HostName} has only {availableDiskPercent}% of {diskEvent.DriveName} drive available!");
                }
            }
        }
    }
}
