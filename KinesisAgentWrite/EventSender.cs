using Newtonsoft.Json;

using NLog;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace KinesisClientWrite
{
    class EventSender
    {
        public EventSender()
        {
            LogManager.Setup().LoadConfiguration(builder => {
                builder.ForLogger().FilterMinLevel(LogLevel.Info).WriteToFile(fileName: @"c:\Events\events.log", layout: "${message}");
            });
        }

        public async Task SendEvents(CancellationToken cancellationToken)
        {
            do
            {
                IEnumerable<Event> drivesInfo = GetEvents();
                IEnumerable<string> events = drivesInfo.Select(driveInfo => JsonConvert.SerializeObject(driveInfo));
                foreach (string driveEvent in events)
                {
                    LogManager.GetCurrentClassLogger().Info(driveEvent);
                }
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
