namespace MongoSmallSizeDBCopy
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using MongoDB.Bson;
    using MongoDB.Driver;
    class Program
    {
        private static MongoClient srcMongoClient;

        private static MongoClient destMongoClient;

        private static List<BsonDocument> insertFailedDocs = new List<BsonDocument>();

        private static int minWait = 1500;
        private static int maxWait = 3000;
        private static long docsCount = 0;

        private static List<CopyItem> copyItems = new List<CopyItem>();

        static void Main(string[] args)
        {

            string srcConnectionString =
               ConfigurationManager.AppSettings["src-conn"];
            MongoClientSettings srcSettings = MongoClientSettings.FromUrl(
                new MongoUrl(srcConnectionString)
            );
            srcMongoClient = new MongoClient(srcSettings);

            string destConnectionString =
                ConfigurationManager.AppSettings["dest-conn"];
            MongoClientSettings destSettings = MongoClientSettings.FromUrl(
                new MongoUrl(destConnectionString)
            );
            destMongoClient = new MongoClient(destSettings);

            LoadDbCopyConfig();

            ProvisionResources();

            CopyData();

            Console.WriteLine("Press enter to exit...");

            Console.ReadLine();
        }

        private static void ProvisionResources()
        {
            if (ConfigurationManager.AppSettings["provision-resources"].ToLower() == "true")
            {
                ProvisionDestinationCollection(destMongoClient);
            }
        }

        private static void CopyData()
        {
            if (ConfigurationManager.AppSettings["migrate-data"].ToLower() == "true")
            {
                foreach (CopyItem c in copyItems)
                {

                    ExportDocuments(c).Wait();

                    if (insertFailedDocs.Any())
                    {
                        using (var sw = new StreamWriter(c.FailedDocsPath))
                        {
                            foreach (var doc in insertFailedDocs)
                            {
                                sw.WriteLine(doc.ToJson());
                            }
                        }

                        Console.WriteLine("Not all documents were exported for source db: {0} - coll: {1} exported, failed documents located @: {2}",
                            c.Sourcedb,
                            c.SourceColl,
                            c.FailedDocsPath);
                    }
                }
            }
        }

        private static void ProvisionDestinationCollection(MongoClient destClient)
        {
            foreach (CopyItem c in copyItems)
            {
                IMongoDatabase destDatabase = destClient.GetDatabase(c.DestinationDb);
                List<string> listOfCollections = destDatabase.ListCollectionNames().ToList();

                if (listOfCollections != null && !listOfCollections.Contains(c.DestinationColl))
                {
                    // Create the collection
                    BsonDocumentCommand<BsonDocument> command =
                        new BsonDocumentCommand<BsonDocument>(
                            new BsonDocument
                            {
                                {"customAction", "CreateCollection"},
                                {"collection", c.DestinationColl},
                                {"shardKey", c.PartitionKey},
                                {"offerThroughput", c.Rus}
                            });
                    BsonDocument response = destDatabase.RunCommandAsync(
                        command).Result;
                    if (response.GetValue("ok").ToInt32() == 1)
                    {
                        Console.WriteLine("Collection: {0} creation successful.", c.DestinationColl);
                    }
                    else
                    {
                        throw new Exception($"Collection: {c.DestinationColl} creation failed");
                    }
                }
            }
        }


        private static void LoadDbCopyConfig()
        {
            using (StreamReader sr = new StreamReader("dbconfig.txt"))
            {
                // Skip header
                sr.ReadLine();
                while (!sr.EndOfStream)
                {
                    string itemRow = sr.ReadLine();
                    if (itemRow != null)
                    {
                        string[] items = itemRow.Split(new char[] { ',' });

                        CopyItem c = new CopyItem();
                        c.Sourcedb = items[0];
                        c.SourceColl = items[1];
                        c.DestinationDb = items[2];
                        c.DestinationColl = items[3];
                        c.Rus = Int32.Parse(items[4]);
                        c.PartitionKey = items[5];
                        c.BatchSize = Int32.Parse(items[6]);
                        c.InsertRetries = Int32.Parse(items[7]);
                        c.FailedDocsPath = items[8];

                        copyItems.Add(c);

                    }
                }
            }

        }

        private static async Task InsertAllDocuments(
            IEnumerable<BsonDocument> docs, 
            CopyItem item,
            IMongoCollection<BsonDocument> destDocStoreCollection)
        {
            var tasks = new List<Task>();
            for (int j = 0; j < docs.Count(); j++)
            {
                tasks.Add(InsertDocument(docs.ToList()[j],item,destDocStoreCollection));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            docsCount = docsCount + docs.Count();
            Console.WriteLine(
                "Total documents copied so far: {0} into destination db: {1}, coll: {2}", 
                docsCount,
                item.DestinationDb,
                item.DestinationColl);
        }

        private static async Task ExportDocuments(
            CopyItem c)
        {
            IMongoDatabase srcDatabase = srcMongoClient.GetDatabase(c.Sourcedb);
            IMongoCollection<BsonDocument> srcDocStoreCollection = srcDatabase.GetCollection<BsonDocument>(c.SourceColl);


            IMongoDatabase destDatabase = destMongoClient.GetDatabase(c.DestinationDb);
            IMongoCollection<BsonDocument> destDocStoreCollection = destDatabase.GetCollection<BsonDocument>(c.DestinationColl);

            FilterDefinition<BsonDocument> filter = FilterDefinition<BsonDocument>.Empty;
            FindOptions<BsonDocument> options = new FindOptions<BsonDocument>
            {
                BatchSize = c.BatchSize,
                NoCursorTimeout = false
            };
            using (IAsyncCursor<BsonDocument> cursor = await srcDocStoreCollection.FindAsync(filter, options))
            {
                bool isSucceed = false;

                while (!isSucceed)
                {

                    try
                    {
                        while (await cursor.MoveNextAsync())
                        {
                            IEnumerable<BsonDocument> batch = cursor.Current;
                            await InsertAllDocuments(batch,c, destDocStoreCollection);
                        }
                    }
                    catch (Exception ex)
                    {

                        if (!IsThrottled(ex))
                        {
                            Console.WriteLine("ERROR: With collection {0}", ex.ToString());
                            throw;
                        }
                        else
                        {
                            // Thread will wait in between 1.5 secs and 3 secs.
                            System.Threading.Thread.Sleep(new Random().Next(minWait, maxWait));
                        }
                    }

                    isSucceed = true;
                    break;
                }
            }

        }


        private static async Task InsertDocument(
            BsonDocument doc,
            CopyItem item,
            IMongoCollection<BsonDocument> destDocStoreCollection)
        {
            bool isSucceed = false;
            for (int i = 0; i < item.InsertRetries; i++)
            {
                try
                {
                    await destDocStoreCollection.InsertOneAsync(doc);

                    isSucceed = true;
                    //Operation succeed just break the loop
                    break;
                }
                catch (Exception ex)
                {

                    if (!IsThrottled(ex))
                    {
                        Console.WriteLine("ERROR: With collection {0}", ex.ToString());
                        throw;
                    }
                    else
                    {
                        // Thread will wait in between 1.5 secs and 3 secs.
                        System.Threading.Thread.Sleep(new Random().Next(minWait, maxWait));
                    }
                }
            }

            if (!isSucceed)
            {
                insertFailedDocs.Add(doc);
            }

        }

        private static bool IsThrottled(Exception ex)
        {
            return ex.Message.ToLower().Contains("Request rate is large".ToLower());
        }
    }
}
