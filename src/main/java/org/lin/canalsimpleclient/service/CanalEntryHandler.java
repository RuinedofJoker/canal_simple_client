package org.lin.canalsimpleclient.service;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.lin.canalsimpleclient.common.CanalClient;

import java.util.*;

public interface CanalEntryHandler {
    void handle(CanalEntry.Entry entry, CanalClient canalClient);

    default boolean isTransactionType(CanalEntry.Entry entry) {
        return (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND);
    }

    default CanalEntry.RowChange getRowChange(CanalEntry.Entry entry) {
        CanalEntry.RowChange rowChange;
        try {
            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
        }
        return rowChange;
    }

    default Map getColumns(List<CanalEntry.Column> columns) {
        if (columns == null || columns.isEmpty()) {
            return new HashMap();
        }
        Map result = new HashMap();
        Map entry = new HashMap();
        Map rowInfo = new HashMap();
        result.put("RowEntry", entry);
        result.put("RowInfo", rowInfo);
        for (CanalEntry.Column column : columns) {
            entry.put(column.getName(), column.getValue());
            Map currentRolInfo = new HashMap();
            currentRolInfo.put("Index", column.getIndex());
            currentRolInfo.put("IsKey", column.getIsKey());
            currentRolInfo.put("IsNull", column.getIsNull());
            currentRolInfo.put("Length", column.getLength());
            currentRolInfo.put("MysqlType", column.getMysqlType());
            currentRolInfo.put("SqlType", column.getSqlType());
            currentRolInfo.put("Updated", column.getUpdated());
            currentRolInfo.put("NameBytes", column.getNameBytes().toByteArray());
            currentRolInfo.put("ValueBytes", column.getValueBytes().toByteArray());
            rowInfo.put(column.getName(), currentRolInfo);
            if (column.getIsKey()) {
                result.put("Key", column.getName());
            }
        }
        return result;
    }

    default Map getBeforeColumns(CanalEntry.RowData rowData) {
        return getColumns(rowData.getBeforeColumnsList());
    }

    default Map getAfterColumns(CanalEntry.RowData rowData) {
        return getColumns(rowData.getAfterColumnsList());
    }

    default Map getEntryInfo(CanalEntry.Entry entry, CanalClient canalClient) {

        Map entryInfo = new HashMap();

        CanalEntry.RowChange rowChange = getRowChange(entry);
        CanalEntry.EventType eventType = rowChange.getEventType();

        entryInfo.put("HeaderByte", entry.getHeader().toByteArray());

        Map header = new HashMap();
        header.put("Gtid", entry.getHeader().getGtid());
        header.put("EventType", entry.getHeader().getEventType());
        header.put("EventLength", entry.getHeader().getEventLength());
        header.put("ExecuteTime", entry.getHeader().getExecuteTime());
        header.put("LogfileName", entry.getHeader().getLogfileName());
        header.put("LogfileOffset", entry.getHeader().getLogfileOffset());
        header.put("SchemaName", entry.getHeader().getSchemaName());
        header.put("TableName", entry.getHeader().getTableName());
        header.put("ServerId", entry.getHeader().getServerId());
        header.put("SourceType", entry.getHeader().getSourceType().toString());
        header.put("ServerenCode", entry.getHeader().getServerenCode());

        entryInfo.put("Header", header);

        entryInfo.put("EventType", eventType.toString());
        entryInfo.put("StoreValueByte", entry.getStoreValue().toByteArray());

        List rows = new LinkedList();
        entryInfo.put("Rows", rows);

        //获取RowChange对象里的每一行数据，打印出来
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            Map row = new HashMap();
            //变更前的数据
            Map beforeColumns = getBeforeColumns(rowData);
            //变更后的数据
            Map afterColumns = getAfterColumns(rowData);
            row.put("BeforeColumns", beforeColumns);
            row.put("AfterColumns", afterColumns);
            rows.add(row);
        }

        entryInfo.put("CanalClientInfo", canalClient);

        return entryInfo;
    }
}
