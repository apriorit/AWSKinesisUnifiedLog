using System;
using System.Threading;

namespace BasicKinesisWrite
{
    class Program
    {
        static void Main()
        {
            var eventSender = new EventSender();
            var cancelationSource = new CancellationTokenSource();
            var eventThread = new Thread(async () => await eventSender.SendEvents(cancelationSource.Token));
            eventThread.Start();
            Console.ReadLine();
            cancelationSource.Cancel();
            eventThread.Join();
        }
    }
}
