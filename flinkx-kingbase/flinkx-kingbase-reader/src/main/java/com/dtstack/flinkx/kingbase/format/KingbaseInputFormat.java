package com.dtstack.flinkx.kingbase.format;

import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.NClob;
import java.util.List;

public class KingbaseInputFormat extends JdbcInputFormat {
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
                    } else if ((obj instanceof java.util.Date
                            || obj.getClass().getSimpleName().toUpperCase().contains("TIMESTAMP"))) {
                        obj = resultSet.getTimestamp(pos + 1);
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
//    @Override
//    protected String getTimeStr(Long location, String incrementColType) {
//        String timeStr;
//        Timestamp ts = new Timestamp(DbUtil.getMillis(location));
//        ts.setNanos(DbUtil.getNanos(location));
//        timeStr = DbUtil.getNanosTimeStr(ts.toString());
//
//        if (ColumnType.TIMESTAMP.name().equals(incrementColType)) {
//            timeStr = String.format("TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS:FF6')", timeStr);
//        } else {
//            timeStr = timeStr.substring(0, 19);
//            timeStr = String.format("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')", timeStr);
//        }
//
//        return timeStr;
//    }
}
