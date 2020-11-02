package com.dtstack.flinkx.udf;

import java.lang.annotation.*;

/**
 * Description for a woven UDF
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface UDF {

    // default function name
    String name();

    // function type: should be UDF1, UDF2, UDF3, UDF4, UDF5
    String fnType() default "UDF1";

    // data type for return value
    String returnType() default "string";

}
