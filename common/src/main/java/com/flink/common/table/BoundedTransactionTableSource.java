package com.flink.common.table;

import com.flink.common.source.TransactionRowInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class BoundedTransactionTableSource extends InputFormatTableSource<Row> {
    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .field("accountId", Types.LONG)
                .field("timestamp", Types.SQL_TIMESTAMP)
                .field("amount", Types.DOUBLE)
                .build();
    }

    @Override
    public DataType getProducedDataType() {
        return getTableSchema().toRowDataType();
    }

    @Override
    public InputFormat<Row, ?> getInputFormat() {
        return new TransactionRowInputFormat();
    }
}
