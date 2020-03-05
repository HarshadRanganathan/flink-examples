## Fraud Detection

Fraud detector processes stream of transactions and outputs alert for any account that makes a small transaction immediately followed by a large one.

### Steps To Run

1. Package artifact ``frauddetection-1.0-SNAPSHOT.jar``
 
2. Upload jar to Flink and submit the job.

![Flink Dashboard](images/flink-dashboard.png?raw=true)

3. Check Task Manager logs for the alerts every few seconds.

```
taskmanager_1           | 2020-03-05 22:24:04,861 INFO  com.flink.common.sink.AlertSink                               - Alert(id=3)
taskmanager_1           | 2020-03-05 22:24:09,870 INFO  com.flink.common.sink.AlertSink                               - Alert(id=3)
taskmanager_1           | 2020-03-05 22:24:14,878 INFO  com.flink.common.sink.AlertSink                               - Alert(id=3)
```