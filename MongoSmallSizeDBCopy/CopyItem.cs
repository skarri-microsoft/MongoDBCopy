namespace MongoSmallSizeDBCopy
{
    class CopyItem
    {
        public string Sourcedb { get; set; }

        public string SourceColl { get; set; }

        public string DestinationDb { get; set; }

        public string DestinationColl { get; set; }

        public int Rus { get; set; }

        public string PartitionKey { get; set; }

        public int BatchSize { get; set; }

        public int InsertRetries { get; set; }

        public string FailedDocsPath { get; set; }

    }
}
