## Output Folder Structure
`checkpoint` is the directory for checkpointing

`ingested` is the directory for ingested output

```bash
├── checkpoint
│   ├── commits
│   │   ├── 0     //3 for each batch: step 4. add a file indicating committed
│   │   ├── 1
│   │   └── 2
│   ├── metadata        //1. mark down the writeStream id(e.g. {"id":"6620ab39-04f9-4af4-8e50-9e5dedff1205"}) as the first thing to do
│   ├── offsets
│   │   ├── 0     //3 for each batch: step 1. mark down the max offset for a topic partition and the configurations for current batch job
│   │   ├── 1
│   │   └── 2
│   └── sources
│       └── 0
│           └── 0       //2. before any batch starts, mark down the beginning offset for topic partitions. e.g. {"quickstart-events":{"0":0},"example-topic":{"2":0,"1":0,"0":0}}
├── ingested
│   ├── _spark_metadata
│   │   ├── 0     //3 for each batch: step 3. keep the output metadata(e.g. filename, size, etc.) in a file with name starting from 0
│   │   ├── 1     //3 for each batch: step 3. a new file will be generated for each batch output
│   │   └── 2
│   ├── part-00000-304e6ea5-368a-485b-ad99-12594bb58de8-c000.json    //3 for each batch: step 2. save the batch output to disk.
│   ├── part-00000-84d67966-b3d7-4e95-8a32-514008a43fc3-c000.json    //3 for each batch: step 2. "part-00000-" is created for each TopicPartition that the streamingQuery is responsible for
│   └── part-00000-adc7631d-98fc-493e-be05-4ea1aa1d5f57-c000.json
```

### File Content Examples
### Spark Metadata file
For `./ingested/kafka-ingest/_spark_metadata/0` file, the content could be
```text
v1
{"path":"file:///.../ingested/kafka-ingest/part-00000-9cdc1546-1ad0-4d11-9c1c-c1425ec99d66-c000.json","size":77,"isDir":false,"modificationTime":1617125637480,"blockReplication":1,"blockSize":33554432,"action":"add"}
{"path":"file:///.../ingested/kafka-ingest/part-00001-cd15d0d5-824c-48d8-8fc9-40f927d86ba2-c000.json","size":150,"isDir":false,"modificationTime":1617125637480,"blockReplication":1,"blockSize":33554432,"action":"add"}
{"path":"file:///.../ingested/kafka-ingest/part-00002-256dac20-ca91-4c2e-acc3-7d9f0eae041f-c000.json","size":67,"isDir":false,"modificationTime":1617125638262,"blockReplication":1,"blockSize":33554432,"action":"add"}
```

### Ingested Json file
A json file is created for each TopicPartition
For `./ingested/kafka-ingest/part-00001-cd15d0d5-824c-48d8-8fc9-40f927d86ba2-c000.json` file, the content could be
```text
{"key":"emp101","data":{"emp_id":101,"first_name":"Mike","last_name":"M"}}
{"key":"emp105","data":{"emp_id":105,"first_name":"Tree","last_name":"T"}}
```
Two rows are ingested in current batch for the Topic example-topic Partition 1.

Be careful that this "part-00001-" does NOT statically map to a TopicPartition. 
The number 00001 indicates how many output files are generated. It's 0-based. 
If there is only one TopicPartition ingested for current batch job, a part-00000- file will be created.
If there are two TopicPartitions ingested for current batch job, 
part-00000- and part-00001- will be created, and they will be referenced in the _spark_metadata file.

In fact, each of the "part-xxxxx-" file is mapped to a Spark task for current micro-batch job, where its metadata
(topic, partition, start-offset, end-offset) has been predefined when the micro-batch job starts by the driver.






















