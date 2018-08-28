# Checkpointing

### Recovering from Failures with Checkpointing 


"In case of a failure or intentional shutdown, you can recover the previous progress and state of a previous query, and continue where it left off. This is done using checkpointing and write ahead logs. You can configure a query with a checkpoint location, and the query will save all the progress information (i.e. range of offsets processed in each trigger) and the running aggregates (e.g. word counts in the quick example) to the checkpoint location. This checkpoint location has to be a path in an HDFS compatible file system, and can be set as an option in the DataStreamWriter when starting a query."

~ Structured streaming programming guide

Example for specifying the checkpoint location:

```Scala
aggDF
  .writeStream
  .outputMode("complete")
  .option("checkpointLocation", "path/to/HDFS/dir")
  .format("memory")
  .start()
  
```

### The checkpoint directory structure

- commits/
  - 0
  - 1
- offsets/
  - 0
  - 1
- state/
  - 0/
    - 0/
      - 1.delta
    - 1/
      - 1.delta
    - 2/
      - 1.delta
    - 3/
      - 1.delta
    - 4/
      - 1.delta
- metadata


### The commits directory

See 

```Scala
  /**
   * A log that records the batch ids that have completed. This is used to check if a batch was
   * fully processed, and its output was committed to the sink, hence no need to process it again.
   * This is used (for instance) during restart, to help identify which batch to run next.
   */
  val commitLog = new CommitLog(sparkSession, checkpointFile("commits"))
```

Example file content:
```
v1
{}
```

So as below the content contains a version and an optional metadata as JSON (in case of MicroBatchExecution it is always empty). 

In the following description commits referenced as completion log and describes how commits are written:

```Scala
/**
 * Used to write log files that represent batch commit points in structured streaming.
 * A commit log file will be written immediately after the successful completion of a
 * batch, and before processing the next batch. Here is an execution summary:
 * - trigger batch 1
 * - obtain batch 1 offsets and write to offset log
 * - process batch 1
 * - write batch 1 to completion log
 * - trigger batch 2
 * - obtain batch 2 offsets and write to offset log
 * - process batch 2
 * - write batch 2 to completion log
 * ....
 *
 * The current format of the batch completion log is:
 * line 1: version
 * line 2: metadata (optional json string)
 */
class CommitLog(sparkSession: SparkSession, path: String)
  extends HDFSMetadataLog[String](sparkSession, path)
```

### The offsets directory

See org.apache.spark.sql.execution.streaming.StreamExecution#offsetLog 

```Scala
  /**
   * A write-ahead-log that records the offsets that are present in each batch. In order to ensure
   * that a given batch will always consist of the same data, we write to this log *before* any
   * processing is done.  Thus, the Nth record in this log indicated data that is currently being
   * processed and the N-1th entry indicates which offsets have been durably committed to the sink.
   */
val offsetLog = new OffsetSeqLog(sparkSession, checkpointFile("offsets"))
```

Example for file content:

```
v1
{"batchWatermarkMs":0,"batchTimestampMs":1534925452068,"conf":{"spark.sql.shuffle.partitions":"200","spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"}}
{"YLYUWXIC":{"2":626,"1":625,"3":625,"0":625}}
```

Where "v1" stands for the current version (Spark constant). 
Then comes the serialization of OffsetSeq, which are

```Scala
/**
 * An ordered collection of offsets, used to track the progress of processing data from one or more
 * [[Source]]s that are present in a streaming query. This is similar to simplified, single-instance
 * vector clock that must progress linearly forward.
 */
case class OffsetSeq(offsets: Seq[Option[Offset]], metadata: Option[OffsetSeqMetadata] = None)
```

The serialization starts with the OffsetSeqMetadata, for details see org.apache.spark.sql.execution.streaming.OffsetSeqMetadata 
(basically they are relevant SQL configurations serialized as json) then comes the offest seq.

Here the example was kafka topic YLYUWXIC with 4 partitions.


### The state directory 

Directory structure: "\<checkpointRootLocation\>/state/\<operatorId\>/\<partitionId\>/\<version\>.\[delta|snapshot\]"  

Design docs: [State Store for Streaming Aggregations](https://docs.google.com/document/d/1-ncawFx8JS5Zyfq1HAEGBx56RDet9wfVp_hDM8ZL254)

States are stored in delta files (its content is compressed, created for each batch) and after a configured interval they are merged into snapshot files.
There is a configuration spark.sql.streaming.minBatchesToRetain (default 100) which controls the cleanup of old delta and snaphots files. 

For each batch when the execution plan is created a new IncrementalExecution instance is created:  

```Scala
new IncrementalExecution(
        sparkSessionToRunBatch,
        triggerLogicalPlan,
        outputMode,
        checkpointFile("state"),
        runId,
        currentBatchId,
        offsetSeqMetadata)

```

Where triggerLogicalPlan is the logical plan of the streaming query.

So the IncrementalExecution is

```Scala
/**
 * A variant of [[QueryExecution]] that allows the execution of the given [[LogicalPlan]]
 * plan incrementally. Possibly preserving state in between each execution.
 */
```

In the IncrementalExecution the planner is overriden with such a spark planner where 
StatefulAggregationStrategy is part of the planning strategies. This leads to calling
org.apache.spark.sql.execution.aggregate.AggUtils#planStreamingAggregation method where
in the root of the query plan a series of new case classes are added

Example (unit test DataStreamReaderWriterSuite.scala # "MemorySink can recover from a checkpoint in Complete Mode"): 

- query (a groupBy and a count on the attribute "a"):

```Scala
    import testImplicits._
    val ms = new MemoryStream[Int](0, sqlContext)
    val df = ms.toDF().toDF("a")
    val tableName = "test"
    def startQuery: StreamingQuery = {
      val writer = df.groupBy("a")
        .count()
        .writeStream
        .format("memory")
        .queryName(tableName)
        .outputMode("complete")
  ...
  ...
      writer.start()

    }
    // no exception here
    val q = startQuery
    ms.addData(0, 1)
    q.processAllAvailable()
    q.stop()
    
```


- incoming query plan in org.apache.spark.sql.execution.aggregate.AggUtils#planStreamingAggregation method:

```
PlanLater Project [value#18 AS a#3]
```

- after the new nodes are added as roots: 

```
HashAggregate(keys=[a#3], functions=[count(1)], output=[a#3, count#7L])
+- StateStoreSave [a#3]
   +- HashAggregate(keys=[a#3], functions=[merge_count(1)], output=[a#3, count#20L])
      +- StateStoreRestore [a#3]
         +- HashAggregate(keys=[a#3], functions=[merge_count(1)], output=[a#3, count#20L])
            +- HashAggregate(keys=[a#3], functions=[partial_count(1)], output=[a#3, count#20L])
               +- PlanLater Project [value#18 AS a#3]
```

These are used by Tungsten and has effect on the aggregation buffers.
Regarding the states the StateStoreRestore and StateStoreSave are two most important case classes. 
These two classes are using the StateStoreProvider to get the store for the statefull aggregation.
A store is like a Map[UnsafeRow, UnsafeRow] where the key is the stands for the fields for 
the aggregation attributes and the value is the full row.


```Scala
/**
 * Trait representing a provider that provide [[StateStore]] instances representing
 * versions of state data.
 *
 * The life cycle of a provider and its provide stores are as follows.
 *
 * - A StateStoreProvider is created in a executor for each unique [[StateStoreId]] when
 *   the first batch of a streaming query is executed on the executor. All subsequent batches reuse
 *   this provider instance until the query is stopped.
 *
 * - Every batch of streaming data request a specific version of the state data by invoking
 *   `getStore(version)` which returns an instance of [[StateStore]] through which the required
 *   version of the data can be accessed. It is the responsible of the provider to populate
 *   this store with context information like the schema of keys and values, etc.
 *
 * - After the streaming query is stopped, the created provider instances are lazily disposed off.
 */
trait StateStoreProvider
```


```Scala
/**
 * An implementation of [[StateStoreProvider]] and [[StateStore]] in which all the data is backed
 * by files in a HDFS-compatible file system. All updates to the store has to be done in sets
 * transactionally, and each set of updates increments the store's version. These versions can
 * be used to re-execute the updates (by retries in RDD operations) on the correct version of
 * the store, and regenerate the store version.
 *
 * Usage:
 * To update the data in the state store, the following order of operations are needed.
 *
 * // get the right store
 * - val store = StateStore.get(
 *      StateStoreId(checkpointLocation, operatorId, partitionId), ..., version, ...)
 * - store.put(...)
 * - store.remove(...)
 * - store.commit()    // commits all the updates to made; the new version will be returned
 * - store.iterator()  // key-value data after last commit as an iterator
 * - store.updates()   // updates made in the last commit as an iterator
 *
 * Fault-tolerance model:
 * - Every set of updates is written to a delta file before committing.
 * - The state store is responsible for managing, collapsing and cleaning up of delta files.
 * - Multiple attempts to commit the same version of updates may overwrite each other.
 *   Consistency guarantees depend on whether multiple attempts have the same updates and
 *   the overwrite semantics of underlying file system.
 * - Background maintenance of files ensures that last versions of the store is always recoverable
 * to ensure re-executed RDD operations re-apply updates on the correct past version of the
 * store.
 */
private[state] class HDFSBackedStateStoreProvider extends StateStoreProvider

```


For analyzing how states are stored the org.apache.spark.sql.execution.streaming.state.StateStore#get method 
is good place to stop. Here the keySchema and the valueSchema is still available. 
As state store deals with UnsafeRow to decode the content to know the schema is important.

```Scala
/**
 * Base trait for a versioned key-value store. Each instance of a `StateStore` represents a specific
 * version of state data, and such instances are created through a [[StateStoreProvider]].
 */
trait StateStore {
...

 /**
   * Get the current value of a non-null key.
   * @return a non-null row if the key exists in the store, otherwise null.
   */
  def get(key: UnsafeRow): UnsafeRow

  /**
   * Put a new value for a non-null key. Implementations must be aware that the UnsafeRows in
   * the params can be reused, and must make copies of the data as needed for persistence.
   */
  def put(key: UnsafeRow, value: UnsafeRow): Unit
...
}
```

StateStoreSaveExec uses mapPartitionsWithStateStore and in our concrete example to see how state 
are written let's extend StateStoreSaveExec with some logs using the schema:

```Scala
val keyFields = mutable.ArrayBuffer.empty[StructField]
keyFields += new StructField("a", IntegerType, false)
val keyConverter =
  CatalystTypeConverters.createToScalaConverter(StructType(keyFields))
val keyAsRow = keyConverter(key)

val fullRowFields = mutable.ArrayBuffer.empty[StructField]
fullRowFields += new StructField("a", IntegerType, false)
fullRowFields += new StructField("count", LongType, false)

val fullRowConverter =
  CatalystTypeConverters.createToScalaConverter(StructType(fullRowFields))
val fullRow = fullRowConverter(row)

// scalastyle:off println
println(s"store.put(${keyAsRow}, ${fullRow}) storeId: ${store.id}")
// scalastyle:on println
```

Running the example the following output is produced:

```
store.put([0], [0,1]) storeId: StateStoreId(file:/private/var/folders/9g/gf583nd1765cvfgb_lsvwgp00000gp/T/streaming.metadata-aef45b34-5016-4616-96d0-da7f8ec1d00b/state,0,1,default)
store.put([1], [1,1]) storeId: StateStoreId(file:/private/var/folders/9g/gf583nd1765cvfgb_lsvwgp00000gp/T/streaming.metadata-aef45b34-5016-4616-96d0-da7f8ec1d00b/state,0,3,default)
```

The [0] and [1] as keys are the grouping attributes, the [0, 1] and [1, 1] is the full row for the corresponding key.

#### Snapshoting 

Snapshots are consolidating delta files to a single file.

So to load the full state the last available snapshot file must be identified (if it is not exists then empty Map[Key, Row]) and deltas should be added up to the current batch.  

Its main config is spark.sql.streaming.stateStore.maintenanceInterval (default 60 sec) specifies the interval for triggering snapshot file creation.

And there is another config spark.sql.streaming.stateStore.minDeltasForSnapshot (default 10) used at maintenance (snapshot file generation).



### The metadata file 

Single file with an UUID generated for the streaming query. 

Example content:

```
{"id":"6a13f719-7949-42c7-97b4-b036f1e7b184"}
```

See org.apache.spark.sql.execution.streaming.StreamExecution#streamMetadata 

```Scala
 /** Metadata associated with the whole query */
  protected val streamMetadata: StreamMetadata = {
    val metadataPath = new Path(checkpointFile("metadata"))
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    StreamMetadata.read(metadataPath, hadoopConf).getOrElse {
      val newMetadata = new StreamMetadata(UUID.randomUUID.toString)
      StreamMetadata.write(newMetadata, metadataPath, hadoopConf)
      newMetadata
    }
  }
```
