using Amazon.Kinesis;
using Amazon.Kinesis.Model;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace BasicKinesisWrite
{
    class EventSender
    {
        AmazonKinesisClient _kinesisClient = new AmazonKinesisClient(new AmazonKinesisConfig());
        public async Task SendEvents(CancellationToken cancellationToken)
        {
            do
            {
                IEnumerable<Event> drivesInfo = GetEvents();
                var requestRecords = new PutRecordsRequest()
                {
                    StreamName = "events",
                    Records = drivesInfo.Select(driveInfo => new PutRecordsRequestEntry
                    {
                        Data = new MemoryStream(JsonSerializer.SerializeToUtf8Bytes(driveInfo)),
                        PartitionKey = driveInfo.EventId
                    }).ToList()
                };
                PutRecordsResponse putResult = await _kinesisClient.PutRecordsAsync(requestRecords, cancellationToken);
                await Task.Delay(10 * 1000);
            }
            while (!cancellationToken.IsCancellationRequested);
        }

        private IEnumerable<Event> GetEvents()
        {
            return DriveInfo.GetDrives().Select(driveInfo => new Event
            {
                EventId = Guid.NewGuid().ToString(),
                HostName = Dns.GetHostName(),
                DriveName = driveInfo.Name,
                TotalSize = driveInfo.TotalSize,
                FreeSpace = driveInfo.TotalFreeSpace,
                AvailableSpace = driveInfo.AvailableFreeSpace
            });
        }
    }
}
