package com.flink.common.source;

import com.flink.common.entity.Transaction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;

public class TransactionRowInputFormat extends GenericInputFormat<Row> implements NonParallelInput {

    private static final long serialVersionUID = 1L;

    private transient Iterator<Transaction> transactions;

    @Override
    public void open(GenericInputSplit split) throws IOException {
        transactions = TransactionIterator.bounded();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !transactions.hasNext();
    }

    @Override
    public Row nextRecord(Row reuse) throws IOException {
        Transaction transaction = transactions.next();
        reuse.setField(0, transaction.getAccountId());
        reuse.setField(1, new Timestamp(transaction.getTimestamp()));
        reuse.setField(2, transaction.getAmount());

        return reuse;
    }
}
