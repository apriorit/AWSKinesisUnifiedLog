using System;
using System.Collections.Generic;
using System.Runtime.Caching;
using System.Text.Json;
using System.Threading;
using Amazon.Kinesis.ClientLibrary;

namespace KCLKinesisRead
{
    class EventsReader : IShardRecordProcessor
    {
        private static readonly TimeSpan _backoff = TimeSpan.FromSeconds(3);
        private static readonly TimeSpan _checkpointInterval = TimeSpan.FromMinutes(5);
        private static readonly int _numRetries = 2;
        private DateTime _nextCheckpointTime = DateTime.UtcNow;

        public void Initialize(InitializationInput input)
        {
            Console.WriteLine($"Shard {input.ShardId} processing was started");
        }

        public void ProcessRecords(ProcessRecordsInput input)
        {
            ProcessRecordsWithRetries(input.Records);
            if (DateTime.UtcNow >= _nextCheckpointTime)
            {
                Checkpoint(input.Checkpointer);
                _nextCheckpointTime = DateTime.UtcNow + _checkpointInterval;
            }
        }

        private void Checkpoint(Checkpointer checkpointer)
        {
            checkpointer.Checkpoint(RetryingCheckpointErrorHandler.Create(_numRetries, _backoff));
        }

        private void ProcessRecordsWithRetries(List<Record> records)
        {
            foreach (Record record in records)
            {
                bool processedSuccessfully = false;
                string data = null;
                for (int i = 0; i < _numRetries; ++i)
                {
                    try
                    {
                        data = System.Text.Encoding.UTF8.GetString(record.Data);
                        var diskEvent = JsonSerializer.Deserialize<Event>(data);
                        long availableDiskPercent = diskEvent.AvailableSpace * 100 / diskEvent.TotalSize;
                        if (availableDiskPercent < 20)
                        {
                            if (MemoryCache.Default.Get($"{diskEvent.HostName}.{diskEvent.DriveName}") == null)
                            {
                                MemoryCache.Default.Set($"{diskEvent.HostName}.{diskEvent.DriveName}", availableDiskPercent, new CacheItemPolicy
                                {
                                    AbsoluteExpiration = DateTime.UtcNow.AddMinutes(20),
                                    RemovedCallback = RemoveCallback
                                });
                            }
                        }
                        else
                        {
                            if (MemoryCache.Default.Get($"{diskEvent.HostName}.{diskEvent.DriveName}") != null)
                            {
                                MemoryCache.Default.Remove($"{diskEvent.HostName}.{diskEvent.DriveName}");
                            }
                        }

                        processedSuccessfully = true;
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine($"Exception processing record data: {data}", ex);
                        Thread.Sleep(_backoff);
                    }
                }

                if (!processedSuccessfully)
                {
                    Console.Error.WriteLine($"Couldn't process record {record}. Skipping the record.");
                }
            }
        }

        private void RemoveCallback(CacheEntryRemovedArguments arguments)
        {
            if (arguments.RemovedReason == CacheEntryRemovedReason.Expired)
            {
                Console.WriteLine("The disk space is going down for 20 minutes");
            }
        }

        public void LeaseLost(LeaseLossInput leaseLossInput)
        {
            Console.Error.WriteLine("Lease lost");
        }

        public void ShardEnded(ShardEndedInput shardEndedInput)
        {
            Console.WriteLine("Lease lost");
            shardEndedInput.Checkpointer.Checkpoint();
        }

        public void ShutdownRequested(ShutdownRequestedInput shutdownRequestedInput)
        {
            Console.Error.WriteLine("Shutdown requested");
            shutdownRequestedInput.Checkpointer.Checkpoint();
        }
    }
}