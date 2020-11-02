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

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class LocalfsInputFormat extends BaseRichInputFormat {

    protected static final long serialVersionUID = 1L;

    protected List<MetaColumn> columns;

    protected transient org.apache.hadoop.mapred.InputFormat inputFormat;

    protected transient RecordReader recordReader;

    protected Object key;

    protected Object value;


    protected String path;

    protected String encoder;

    private ExecutorService executorService;

    private WatchService watchService = null;

    private Path dir;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        this.inputFormat = new TextInputFormat();
        try {
            executorService = Executors.newFixedThreadPool(1);
            watchService = FileSystems.getDefault().newWatchService();
            dir = Paths.get(path);
            if (Files.notExists(dir) || !Files.isDirectory(dir)) {
                LOG.error("dir does not exist");
                throw new RuntimeException("dir does not exist");
            }
            dir.register(watchService, ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);
        } catch (NumberFormatException | IOException e) {
            LOG.error("setup file system watch service failed", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        LocalInputSplit hdfsTextInputSplit = (LocalInputSplit) inputSplit;
        org.apache.hadoop.mapred.InputSplit fileSplit = hdfsTextInputSplit.getTextSplit();
//        findCurrentPartition(((FileSplit) fileSplit).getPath());
        recordReader = inputFormat.getRecordReader(fileSplit, new JobConf(), Reporter.NULL);
        key = new LongWritable();
        value = new Text();

    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        String line = new String(((Text)value).getBytes(), 0, ((Text)value).getLength(), encoder);
        String[] fields = StringUtils.splitPreserveAllTokens(line, ",");

        if (columns.size() == 1 && ConstantValue.STAR_SYMBOL.equals(columns.get(0).getName())){
            row = new Row(fields.length);
            for (int i = 0; i < fields.length; i++) {
                row.setField(i, fields[i]);
            }
        } else {
            row = new Row(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                MetaColumn metaColumn = columns.get(i);

                Object value = null;
                if(metaColumn.getValue() != null){
                    value = metaColumn.getValue();
                } else if(metaColumn.getIndex() != null && metaColumn.getIndex() < fields.length){
                    String strVal = fields[metaColumn.getIndex()];
                    if (!"".equals(strVal)){
                        value = strVal;
                    }
                }

                if(value != null){
                    value = StringUtil.string2col(String.valueOf(value), metaColumn.getType(),metaColumn.getTimeFormat());
                }

                row.setField(i, value);
            }
        }

        return row;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        key = new LongWritable();
        value = new Text();
        return !recordReader.next(key, value);
    }

    @Override
    protected void closeInternal() throws IOException {
        if(recordReader != null) {
            recordReader.close();
        }
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
        JobConf jobConf = new JobConf();
        String inputPath = "file://" + path;
        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(jobConf, inputPath);
        TextInputFormat inputFormat = new TextInputFormat();

//        jobConf.set("mapreduce.input.fileinputformat.input.dir.recursive","true");
        inputFormat.configure(jobConf);
        org.apache.hadoop.mapred.InputSplit[] splits = inputFormat.getSplits(jobConf, minNumSplits);

        if(splits != null) {
            LocalInputSplit[] localInputSplits = new LocalInputSplit[splits.length];
            for (int i = 0; i < splits.length; ++i) {
                localInputSplits[i] = new LocalInputSplit(splits[i], i);
            }
            return localInputSplits;
        }

        return null;
    }


    static class LocalInputSplit implements InputSplit {
        int splitNumber;
        byte[] textSplitData;

        public LocalInputSplit(org.apache.hadoop.mapred.InputSplit split, int splitNumber) throws IOException {
            this.splitNumber = splitNumber;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            split.write(dos);
            textSplitData = baos.toByteArray();
            baos.close();
            dos.close();
        }

        public org.apache.hadoop.mapred.InputSplit getTextSplit() throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(textSplitData);
            DataInputStream dis = new DataInputStream(bais);
            org.apache.hadoop.mapred.InputSplit split = new FileSplit(null, 0L, 0L, (String[])null);
            split.readFields(dis);
            bais.close();
            dis.close();
            return split;
        }

        @Override
        public int getSplitNumber() {
            return splitNumber;
        }
    }
}
