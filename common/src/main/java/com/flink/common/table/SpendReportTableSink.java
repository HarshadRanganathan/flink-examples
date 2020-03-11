package com.flink.common.table;

import com.flink.common.sink.LoggerOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class SpendReportTableSink implements AppendStreamTableSink<Row>, BatchTableSink<Row> {

    private final TableSchema schema;

    public SpendReportTableSink() {
        this.schema = TableSchema
                .builder()
                .field("accountId", Types.LONG)
                .field("timestamp", Types.SQL_TIMESTAMP)
                .field("amount", Types.DOUBLE)
                .build();
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {

    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream
                .map(SpendReportTableSink::format)
                .writeUsingOutputFormat(new LoggerOutputFormat())
                .setParallelism(dataStream.getParallelism());
    }

    @Override
    public DataType getConsumedDataType() {
        return getTableSchema().toRowDataType();
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return this;
    }

    private static String format(Row row) {
        //noinspection MalformedFormatString
        return String.format("%s, %s, $%.2f", row.getField(0), row.getField(1), row.getField(2));
    }

    @Override
    public void emitDataSet(DataSet<Row> dataSet) {
        dataSet
                .map(SpendReportTableSink::format)
                .output(new LoggerOutputFormat());
    }
}
