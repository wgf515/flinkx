/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.localfs.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LocalfsReader extends BaseDataReader {
    private static Logger LOG = LoggerFactory.getLogger(LocalfsReader.class);

    private List<MetaColumn> columns;

    private String path;

    private String encoder;

    public LocalfsReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        path = readerConfig.getParameter().getStringVal("path");
        encoder = readerConfig.getParameter().getStringVal("encoder", "UTF-8");
        columns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn());
    }

    @Override
    public DataStream<Row> readData() {
        LocalfsInputFormatBuilder builder = new LocalfsInputFormatBuilder();
        builder.setPath(path);
        builder.setEncoder(encoder);
        builder.setColumns(columns);
        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);
        builder.setRestoreConfig(restoreConfig);
        builder.setTestConfig(testConfig);
        builder.setLogConfig(logConfig);

        return createInput(builder.finish());
    }
}
