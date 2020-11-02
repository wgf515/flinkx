package com.dtstack.flinkx.udf.internal;

import com.dtstack.flinkx.udf.UDF;
import org.apache.flink.table.functions.ScalarFunction;

@UDF(name = "MIX")
public class MixUDF extends ScalarFunction {

    public String eval(String string) {
        return "*";
    }

}
