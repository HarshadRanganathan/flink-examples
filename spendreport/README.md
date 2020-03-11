## Spend Report

Continuous ETL pipeline for tracking financial transactions by account over time.

This project contains both batch and streaming application code.

Streaming application makes use of ``EventTime`` and ``TumblingWindow`` with ``BoundedOutOfOrderTimestamps`` watermarking strategy
to ensure that out of order transactions are processed within the time window of one hour.
 
Output is a stream of aggregated transaction amounts by accounts.

### Steps To Run

1. Package artifact ``spendreport-1.0-SNAPSHOT.jar``
 
2. Upload jar to Flink and submit the job.

3. Check Task Manager logs for the aggregated transaction amount by accounts over a time window of one hour.

```
2020-03-11 22:02:06,967 INFO com.flink.common.sink.LoggerOutputFormat - 2, 2019-02-16 17:00:00.0, $810.06

2020-03-11 22:02:06,968 INFO com.flink.common.sink.LoggerOutputFormat - 1, 2019-02-16 17:00:00.0, $686.87

2020-03-11 22:02:06,968 INFO com.flink.common.sink.LoggerOutputFormat - 4, 2019-02-16 17:00:00.0, $254.04

2020-03-11 22:02:06,968 INFO com.flink.common.sink.LoggerOutputFormat - 5, 2019-02-16 17:00:00.0, $762.27

2020-03-11 22:02:06,970 INFO com.flink.common.sink.LoggerOutputFormat - 3, 2019-02-16 17:00:00.0, $0.99

2020-03-11 22:02:07,971 INFO com.flink.common.sink.LoggerOutputFormat - 2, 2019-02-16 18:00:00.0, $458.40

2020-03-11 22:02:07,971 INFO com.flink.common.sink.LoggerOutputFormat - 1, 2019-02-16 18:00:00.0, $859.35

2020-03-11 22:02:07,971 INFO com.flink.common.sink.LoggerOutputFormat - 4, 2019-02-16 18:00:00.0, $415.08

2020-03-11 22:02:07,972 INFO com.flink.common.sink.LoggerOutputFormat - 5, 2019-02-16 18:00:00.0, $206.98

2020-03-11 22:02:07,972 INFO com.flink.common.sink.LoggerOutputFormat - 3, 2019-02-16 18:00:00.0, $871.95

2020-03-11 22:02:08,974 INFO com.flink.common.sink.LoggerOutputFormat - 2, 2019-02-16 19:00:00.0, $730.02

2020-03-11 22:02:08,974 INFO com.flink.common.sink.LoggerOutputFormat - 1, 2019-02-16 19:00:00.0, $330.85

2020-03-11 22:02:08,975 INFO com.flink.common.sink.LoggerOutputFormat - 4, 2019-02-16 19:00:00.0, $523.54

2020-03-11 22:02:08,975 INFO com.flink.common.sink.LoggerOutputFormat - 5, 2019-02-16 19:00:00.0, $605.53
```