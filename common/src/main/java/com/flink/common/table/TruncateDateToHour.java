package com.flink.common.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.table.functions.ScalarFunction;

public class TruncateDateToHour extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    private static final long ONE_HOUR = 60 * 60 * 1000;

    /**
     * Behavior of a ScalarFunction can be defined by implementing a custom evaluation method.
     * An evaluation method must be declared publicly and named "eval".
     * Evaluation methods can also be overloaded by implementing multiple methods named "eval".
     * @param timestamp
     * @return
     */
    public long eval(long timestamp) {
        return timestamp - (timestamp % ONE_HOUR);
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.SQL_TIMESTAMP();
    }
}
