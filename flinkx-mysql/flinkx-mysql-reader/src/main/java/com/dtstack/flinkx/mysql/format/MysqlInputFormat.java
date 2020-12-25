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
package com.dtstack.flinkx.mysql.format;

import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Clob;
import java.sql.NClob;
import java.util.List;

/**
 * Date: 2019/09/19
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class MysqlInputFormat extends JdbcInputFormat {
    private List<Integer> columnTypeList;

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        // 避免result.next阻塞
        if(incrementConfig.isPolling() && StringUtils.isEmpty(incrementConfig.getStartLocation()) && fetchSize==databaseInterface.getFetchSize()){
            fetchSize = 1000;

        }
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
                    } else if (CollectionUtils.isNotEmpty(descColumnTypeList)) {
                        String columnType = descColumnTypeList.get(pos);
                        if ("year".equalsIgnoreCase(columnType)) {
                            java.util.Date date = (java.util.Date) obj;
                            obj = DateUtil.dateToYearString(date);
                        } else if ("tinyint".equalsIgnoreCase(columnType) || "bit".equalsIgnoreCase(columnType)) {
                            if (obj instanceof Boolean) {
                                obj = ((Boolean) obj ? 1 : 0);
                            }
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

}
