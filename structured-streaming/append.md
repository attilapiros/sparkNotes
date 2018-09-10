# Append output mode

> Append mode (default) - This is the default mode, where only the new rows added to the Result Table since the last trigger will be outputted to the sink. This is supported for only those queries where rows added to the Result Table is never going to change. Hence, this mode guarantees that each row will be output only once (assuming fault-tolerant sink). For example, queries with only select, where, map, flatMap, filter, join, etc. will support Append mode.
>
> ~  Structured Streaming Programming Guide

So supported queries:

- Queries with aggregation. **Only when** the aggregation is on event-time of the watermark (or a window on the event-time) => withWatermark called before the aggregation using the same column.

  - Example: [groupBy Streaming Aggregation with Append Output Mode](https://jaceklaskowski.gitbooks.io/spark-structured-streaming/spark-sql-streaming-demo-groupBy-aggregation-append.html).

  - Unit test: org.apache.spark.sql.streaming.EventTimeWatermarkSuite#test("append mode")
    Here the first CheckNewAnswer is empty as with append mode watermarking in window based aggregation 
    the partial counts are not updated to the Result Table and not written to sink. 
    Sink is updated when the window is later then the watermark and intermediate state can be dropped.

```Scala
  test("append mode") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 10, 11, 12, 13, 14, 15),
      CheckNewAnswer(),
      AddData(inputData, 25),   // Advance watermark to 15 seconds
      CheckNewAnswer((10, 5)),
      assertNumStateRows(2),
      AddData(inputData, 10),   // Should not emit anything as data less than watermark
      CheckNewAnswer(),
      assertNumStateRows(2)
    )
  }
```

- Queries with flatMapGroupsWithState (after a groupByKey) . 

  - Example: [flatMapGroupsWithState Operator — Arbitrary Stateful Streaming Aggregation](https://jaceklaskowski.gitbooks.io/spark-structured-streaming/spark-sql-streaming-KeyValueGroupedDataset-flatMapGroupsWithState.html)

  - Unit test: it is not tested directly but the follwing test is a modified version of:
  org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite#testWithAllStateVersions("flatMapGroupsWithState - streaming + aggregation") 

```Scala
  testWithAllStateVersions("MODIFIED flatMapGroupsWithState - streaming + aggregation") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {

      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        Iterator(key -> "-1")
      } else {
        state.update(RunningCount(count))
        Iterator(key -> count.toString)
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(Append, GroupStateTimeout.NoTimeout)(stateFunc)

    testStream(result, Append)(
      AddData(inputData, "a"),
      CheckNewAnswer(("a", "1")),
      AddData(inputData, "a", "b"),
      CheckNewAnswer(("a", "2"), ("b", "1")),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"),
      CheckNewAnswer(("a", "-1"), ("b", "2")),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"),
      CheckNewAnswer(("a", "1"), ("c", "1"))
    )
  }
```
- Queries with joins

  - Unit test: org.apache.spark.sql.streaming.StreamSuite#test("join")
    ( the second arg of testStream is an outputMode which default value is Append)

```Scala
 test("join") {
    // Make a table and ensure it will be broadcast.
    val smallTable = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")

    // Join the input stream with a table.
    val inputData = MemoryStream[Int]
    val joined = inputData.toDS().toDF().join(smallTable, $"value" === $"number")

    testStream(joined)(
      AddData(inputData, 1, 2, 3),
      CheckAnswer(Row(1, 1, "one"), Row(2, 2, "two")),
      AddData(inputData, 4),
      CheckAnswer(Row(1, 1, "one"), Row(2, 2, "two"), Row(4, 4, "four")))
  }

```

- Other simple queries with select, where, filter, map, flatmap,... 

