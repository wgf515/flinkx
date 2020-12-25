package com.dtstack.flinkx.udf.internal;

import com.dtstack.flinkx.udf.UDF;
import org.apache.flink.table.functions.ScalarFunction;

@UDF(name = "BLANK")
public class BlankUDF extends ScalarFunction {

    public String eval(String string) {
        return "";
    }

}
