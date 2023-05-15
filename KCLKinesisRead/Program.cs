using Amazon.Kinesis.ClientLibrary;

using System;

namespace KCLKinesisRead
{
    class Program
    {
        public static void Main(string[] args)
        {
            try
            {
                KclProcess.Create(new EventsReader()).Run();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("ERROR: " + e);
            }
        }
    }
}
