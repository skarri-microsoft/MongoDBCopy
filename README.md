# Copy data from Source DB to Destination DB

## dbconfig.txt
* Enter one row for each collection you want to migrate in the following format.(Columns are self explainable)

    * sourcedb,sourcecoll,destinationdb,destinationcoll,rus,partitionkey,batchsize,insertRetries,failedDocsPath

Note:  Header must be present and order of columns can not be changed.

## app.config

* src-conn - source connection.
* dest-conn - destination connection.
* provision-resources - specify true if you want to create resources through code.
* migrate-data - specify true if you want to migrate data from each source collection to destination collection.

Important: This is not a scalable solution, which works only for tiny collections and copy process is sequential and slow.
