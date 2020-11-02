package com.dtstack.flinkx.udf;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by P0007 on 2019/10/17.
 */
public class UserDefinedFunctionRegistry {

    private static final Logger logger = LoggerFactory.getLogger(UserDefinedFunctionRegistry.class);

    private StreamTableEnvironment tableContext;

    private static String internalUDFPackage = "com.dtstack.flinkx.udf.internal";

    public UserDefinedFunctionRegistry(StreamTableEnvironment tableContext) {
        this.tableContext = tableContext;
    }

    private static Map<String, Object> cacheUdfs = new HashMap<>();

    static {
        new FastClasspathScanner(internalUDFPackage).matchClassesWithAnnotation(UDF.class, aClass -> {
            try {
                UDF udfAnnotation = aClass.getAnnotation(UDF.class);
                String funcName = udfAnnotation.name();
                Object newInstance = aClass.newInstance();
                cacheUdfs.put(funcName, newInstance);
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Try to scan internal UDFs throw exception", e);
            }
        }).scan();
    }


    /*
    *
    * FastClasspathScanner如果多次扫描，会报OutOfMemoryError
    * */
    public void registerInternalUDFs() {
        for (String funcName : cacheUdfs.keySet()) {
            Object newInstance = cacheUdfs.get(funcName);
            registerUDF(funcName, newInstance);
            logger.info("Register UDF: {}", funcName);
        }
    }

    public void registerUDF(String func, Object udf) {
        if (udf instanceof ScalarFunction) {
            tableContext.registerFunction(func, (ScalarFunction) udf);
        } else if (udf instanceof TableFunction) {
            tableContext.registerFunction(func, (TableFunction) udf);
        } else if (udf instanceof AggregateFunction) {
            tableContext.registerFunction(func, (AggregateFunction) udf);
        } else {
            throw new RuntimeException("User defined function type not supported");
        }

    }
}
