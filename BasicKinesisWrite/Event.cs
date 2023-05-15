namespace BasicKinesisWrite
{
    class Event
    {
        public string EventId { get; set; }
        public string HostName { get; set; }
        public string DriveName { get; set; }
        public long TotalSize { get; set; }
        public long FreeSpace { get; set; }
        public long AvailableSpace { get; set; }
    }
}
