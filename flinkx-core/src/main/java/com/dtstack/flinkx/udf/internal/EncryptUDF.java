package com.dtstack.flinkx.udf.internal;

import com.dtstack.flinkx.udf.UDF;
import org.apache.flink.table.functions.ScalarFunction;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

@UDF(name = "ENCRYPT")
public class EncryptUDF extends ScalarFunction {

    public String eval(String string, String key) {
        StandardPBEStringEncryptor stringEncryptor = new StandardPBEStringEncryptor();
        stringEncryptor.setAlgorithm("PBEWithMD5AndDES");          // 加密的算法，这个算法是默认的
        stringEncryptor.setPassword(key); // 加密的密钥,可以自定义,但必须和配置文件中配置的解密密钥一致
        return stringEncryptor.encrypt(string);
    }

}
