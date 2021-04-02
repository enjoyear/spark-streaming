
## Folder structure
The folder structure for the delta table path `/tmp/delta-table`
tutorial https://docs.delta.io/latest/quick-start.html#quickstart
```text
├── _delta_log
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   └── 00000000000000000002.json
├── part-00000-2e8fac88-36c3-4d97-9193-7a7da46f1f0f-c000.snappy.parquet
├── part-00000-34efe815-5058-42b6-94e2-e501ef30bfb2-c000.snappy.parquet
├── part-00000-ca171552-994d-49ed-8095-5c26706581b4-c000.snappy.parquet
├── part-00001-1c15b6d4-cc65-4c6f-8f38-735fb18e3ff9-c000.snappy.parquet
├── part-00001-8db0f530-95b2-4396-8227-e029b8df29fa-c000.snappy.parquet
├── part-00001-c5ec66bc-16e3-4f7c-a5a0-7864cf8af65f-c000.snappy.parquet
├── part-00003-565df42c-bf3f-4e9a-a1b7-48cce303d17a-c000.snappy.parquet
├── part-00003-cd2220bf-eadf-4702-8a66-4e0f7377ec19-c000.snappy.parquet
├── part-00004-401acfaa-1869-43f0-80cc-8e059ffb9821-c000.snappy.parquet
├── part-00004-72b56444-353c-465f-9f8c-d0953cba2c1f-c000.snappy.parquet
├── part-00006-566cbc05-d4c5-46cd-b074-8f9275b2c75d-c000.snappy.parquet
├── part-00006-9f665dec-d133-4dcb-8faa-6da6ae633241-c000.snappy.parquet
├── part-00007-145d30eb-4599-412c-a267-baaf36040992-c000.snappy.parquet
└── part-00007-d64e6fa7-5a6f-42ec-b6b5-90723acb1624-c000.snappy.parquet
```
For each change on the delta table path `/tmp/delta-table`, a new `0000xxx.json` file will be generated.
For example
- File `00000000000000000000.json`
```json
{"commitInfo":{"timestamp":1617253719966,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isBlindAppend":true,"operationMetrics":{"numFiles":"6","numOutputBytes":"2611","numOutputRows":"5"}}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"f700412b-5e35-4f35-afb1-c3968c40d1e9","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1617253719281}}
{"add":{"path":"part-00000-ca171552-994d-49ed-8095-5c26706581b4-c000.snappy.parquet","partitionValues":{},"size":296,"modificationTime":1617253719929,"dataChange":true}}
{"add":{"path":"part-00001-1c15b6d4-cc65-4c6f-8f38-735fb18e3ff9-c000.snappy.parquet","partitionValues":{},"size":463,"modificationTime":1617253719930,"dataChange":true}}
{"add":{"path":"part-00003-565df42c-bf3f-4e9a-a1b7-48cce303d17a-c000.snappy.parquet","partitionValues":{},"size":463,"modificationTime":1617253719930,"dataChange":true}}
{"add":{"path":"part-00004-401acfaa-1869-43f0-80cc-8e059ffb9821-c000.snappy.parquet","partitionValues":{},"size":463,"modificationTime":1617253719929,"dataChange":true}}
{"add":{"path":"part-00006-566cbc05-d4c5-46cd-b074-8f9275b2c75d-c000.snappy.parquet","partitionValues":{},"size":463,"modificationTime":1617253719929,"dataChange":true}}
{"add":{"path":"part-00007-d64e6fa7-5a6f-42ec-b6b5-90723acb1624-c000.snappy.parquet","partitionValues":{},"size":463,"modificationTime":1617253719929,"dataChange":true}}
```
To read this version
```scala
scala> spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table").show
+---+
| id|
+---+
|  1|
|  0|
|  2|
|  3|
|  4|
+---+
```

- File `00000000000000000001.json`
```json
{"commitInfo":{"timestamp":1617254273311,"operation":"WRITE","operationParameters":{"mode":"Overwrite","partitionBy":"[]"},"readVersion":0,"isBlindAppend":false,"operationMetrics":{"numFiles":"6","numOutputBytes":"2611","numOutputRows":"5"}}}
{"add":{"path":"part-00000-34efe815-5058-42b6-94e2-e501ef30bfb2-c000.snappy.parquet","partitionValues":{},"size":296,"modificationTime":1617254272640,"dataChange":true}}
{"add":{"path":"part-00001-8db0f530-95b2-4396-8227-e029b8df29fa-c000.snappy.parquet","partitionValues":{},"size":463,"modificationTime":1617254272643,"dataChange":true}}
{"add":{"path":"part-00003-cd2220bf-eadf-4702-8a66-4e0f7377ec19-c000.snappy.parquet","partitionValues":{},"size":463,"modificationTime":1617254272641,"dataChange":true}}
{"add":{"path":"part-00004-72b56444-353c-465f-9f8c-d0953cba2c1f-c000.snappy.parquet","partitionValues":{},"size":463,"modificationTime":1617254272641,"dataChange":true}}
{"add":{"path":"part-00006-9f665dec-d133-4dcb-8faa-6da6ae633241-c000.snappy.parquet","partitionValues":{},"size":463,"modificationTime":1617254272641,"dataChange":true}}
{"add":{"path":"part-00007-145d30eb-4599-412c-a267-baaf36040992-c000.snappy.parquet","partitionValues":{},"size":463,"modificationTime":1617254272641,"dataChange":true}}
{"remove":{"path":"part-00003-565df42c-bf3f-4e9a-a1b7-48cce303d17a-c000.snappy.parquet","deletionTimestamp":1617254273307,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":463}}
{"remove":{"path":"part-00001-1c15b6d4-cc65-4c6f-8f38-735fb18e3ff9-c000.snappy.parquet","deletionTimestamp":1617254273307,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":463}}
{"remove":{"path":"part-00004-401acfaa-1869-43f0-80cc-8e059ffb9821-c000.snappy.parquet","deletionTimestamp":1617254273307,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":463}}
{"remove":{"path":"part-00000-ca171552-994d-49ed-8095-5c26706581b4-c000.snappy.parquet","deletionTimestamp":1617254273307,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":296}}
{"remove":{"path":"part-00006-566cbc05-d4c5-46cd-b074-8f9275b2c75d-c000.snappy.parquet","deletionTimestamp":1617254273307,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":463}}
{"remove":{"path":"part-00007-d64e6fa7-5a6f-42ec-b6b5-90723acb1624-c000.snappy.parquet","deletionTimestamp":1617254273307,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":463}}```
```
To read this version
```scala
scala> spark.read.format("delta").option("versionAsOf", 1).load("/tmp/delta-table").show
+---+
| id|
+---+
|  8|
|  9|
|  5|
|  7|
|  6|
+---+
```

- File `00000000000000000002.json`
```json
{"commitInfo":{"timestamp":1617254360974,"operation":"UPDATE","operationParameters":{"predicate":"((id#7053L % 2) = 0)"},"readVersion":1,"isBlindAppend":false,"operationMetrics":{"numRemovedFiles":"2","numAddedFiles":"2","numUpdatedRows":"2","numCopiedRows":"0"}}}
{"remove":{"path":"part-00006-9f665dec-d133-4dcb-8faa-6da6ae633241-c000.snappy.parquet","deletionTimestamp":1617254360369,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":463}}
{"remove":{"path":"part-00003-cd2220bf-eadf-4702-8a66-4e0f7377ec19-c000.snappy.parquet","deletionTimestamp":1617254360369,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":463}}
{"add":{"path":"part-00000-2e8fac88-36c3-4d97-9193-7a7da46f1f0f-c000.snappy.parquet","partitionValues":{},"size":463,"modificationTime":1617254360953,"dataChange":true}}
{"add":{"path":"part-00001-c5ec66bc-16e3-4f7c-a5a0-7864cf8af65f-c000.snappy.parquet","partitionValues":{},"size":463,"modificationTime":1617254360953,"dataChange":true}}```
To read this version
```scala
scala> spark.read.format("delta").option("versionAsOf", 2).load("/tmp/delta-table").show
+---+
| id|
+---+
|108|
|  9|
|  5|
|106|
|  7|
+---+
```






