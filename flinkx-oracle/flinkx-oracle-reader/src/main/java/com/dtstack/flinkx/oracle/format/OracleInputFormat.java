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
package com.dtstack.flinkx.oracle.format;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Timestamp;
import java.util.List;

/**
 * Date: 2019/09/19
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class OracleInputFormat extends JdbcInputFormat {
    private List<Integer> columnTypeList;

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        super.openInternal(inputSplit);
        columnTypeList = DbUtil.analyzeColumn(resultSet);
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        if (!hasNext) {
            return null;
        }
        row = new Row(columnCount);

        try {
            for (int pos = 0; pos < row.getArity(); pos++) {
                Object obj = resultSet.getObject(pos + 1);
                if (obj != null) {
                    if (DbUtil.isBinaryType(columnTypeList.get(pos))) {
                        obj = resultSet.getBytes(pos + 1);
                    } else if (DbUtil.isClobType(columnTypeList.get(pos))) {
                        Clob clob = resultSet.getClob(pos + 1);
                        obj = clob.getSubString(1, (int) clob.length());
                    } else if (DbUtil.isNclobType(columnTypeList.get(pos))) {
                        NClob nClob = resultSet.getNClob(pos + 1);
                        obj = nClob.getSubString(1, (int) nClob.length());
                    } else if (obj instanceof BigDecimal) {
                        switch (metaColumns.get(pos).getType().toLowerCase()) {
                            case "int":
                                obj = resultSet.getInt(pos + 1);
                                break;
                            case "bigint":
                                obj = resultSet.getLong(pos + 1);
                                break;
                            case "short":
                                obj = resultSet.getShort(pos + 1);
                                break;
                            case "double":
                                obj = resultSet.getDouble(pos + 1);
                                break;
                            case "float":
                                obj = resultSet.getFloat(pos + 1);
                                break;
                        }
                    } else {
                        if ((obj instanceof java.util.Date || obj.getClass().getSimpleName().toUpperCase().contains("TIMESTAMP"))) {
                            obj = resultSet.getTimestamp(pos + 1);
                        }
                    }
                }
                row.setField(pos, obj);
            }
            return super.nextRecordInternal(row);
        } catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }

    }

    /**
     * 构建时间边界字符串
     *
     * @param location         边界位置(起始/结束)
     * @param incrementColType 增量字段类型
     * @return
     */
    @Override
    protected String getTimeStr(Long location, String incrementColType) {
        String timeStr;
        Timestamp ts = new Timestamp(DbUtil.getMillis(location));
        ts.setNanos(DbUtil.getNanos(location));
        timeStr = DbUtil.getNanosTimeStr(ts.toString());


        if (ColumnType.TIMESTAMP.name().toLowerCase().equals(incrementColType.toLowerCase())) {
            //纳秒精度为9位
            timeStr = String.format("TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS:FF9')", timeStr);
        } else {
            timeStr = timeStr.substring(0, 19);
            timeStr = String.format("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')", timeStr);
        }

        return timeStr;
    }
}
