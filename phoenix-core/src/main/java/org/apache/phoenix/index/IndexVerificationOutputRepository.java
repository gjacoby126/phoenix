package org.apache.phoenix.index;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.mapreduce.index.IndexTool;

import java.io.IOException;

public class IndexVerificationOutputRepository {
    public static final byte[] ROW_KEY_SEPARATOR_BYTE = Bytes.toBytes("|");
    private Table indexHTable;
    private byte[] indexName;
    private Table outputHTable;

    public IndexVerificationOutputRepository(byte[] indexName, HTableFactory hTableFactory) throws IOException {
        this.indexName = indexName;
        outputHTable = hTableFactory.getTable(new ImmutableBytesPtr(IndexTool.RESULT_TABLE_NAME_BYTES));
        indexHTable = hTableFactory.getTable(new ImmutableBytesPtr(indexName));
    }

    public static byte[] generateOutputTableRowKey(long ts, byte[] indexTableName, byte[] dataRowKey ) {
        byte[] keyPrefix = Bytes.toBytes(Long.toString(ts));
        byte[] rowKey;
        int targetOffset = 0;
        // The row key for the output table : timestamp | index table name | data row key
        rowKey = new byte[keyPrefix.length + ROW_KEY_SEPARATOR_BYTE.length + indexTableName.length +
            ROW_KEY_SEPARATOR_BYTE.length + dataRowKey.length];
        Bytes.putBytes(rowKey, targetOffset, keyPrefix, 0, keyPrefix.length);
        targetOffset += keyPrefix.length;
        Bytes.putBytes(rowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(rowKey, targetOffset, indexTableName, 0, indexTableName.length);
        targetOffset += indexTableName.length;
        Bytes.putBytes(rowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(rowKey, targetOffset, dataRowKey, 0, dataRowKey.length);
        return rowKey;
    }

    @VisibleForTesting
    public void logToIndexToolOutputTable(byte[] dataRowKey, byte[] indexRowKey, long dataRowTs,
                                          long indexRowTs,
                                          String errorMsg, byte[] expectedValue, byte[] actualValue,
                                          Scan scan, byte[] tableName, boolean isBeforeRebuild)
        throws IOException {
        final byte[] E_VALUE_PREFIX_BYTES = Bytes.toBytes(" E:");
        final byte[] A_VALUE_PREFIX_BYTES = Bytes.toBytes(" A:");
        final int PREFIX_LENGTH = 3;
        final int TOTAL_PREFIX_LENGTH = 6;
        final byte[] PHASE_BEFORE_VALUE = Bytes.toBytes("BEFORE");
        final byte[] PHASE_AFTER_VALUE = Bytes.toBytes("AFTER");
        long scanMaxTs = scan.getTimeRange().getMax();
        byte[] rowKey = generateOutputTableRowKey(scanMaxTs, indexHTable.getName().toBytes(), dataRowKey);
        Put put = new Put(rowKey);
        put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.DATA_TABLE_NAME_BYTES,
            scanMaxTs, tableName);
        put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.INDEX_TABLE_NAME_BYTES,
            scanMaxTs, indexName);
        put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.DATA_TABLE_TS_BYTES,
            scanMaxTs, Bytes.toBytes(Long.toString(dataRowTs)));

        put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.INDEX_TABLE_ROW_KEY_BYTES,
            scanMaxTs, indexRowKey);
        put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.INDEX_TABLE_TS_BYTES,
            scanMaxTs, Bytes.toBytes(Long.toString(indexRowTs)));
        byte[] errorMessageBytes;
        if (expectedValue != null) {
            errorMessageBytes = new byte[errorMsg.length() + expectedValue.length + actualValue.length +
                TOTAL_PREFIX_LENGTH];
            Bytes.putBytes(errorMessageBytes, 0, Bytes.toBytes(errorMsg), 0, errorMsg.length());
            int length = errorMsg.length();
            Bytes.putBytes(errorMessageBytes, length, E_VALUE_PREFIX_BYTES, 0, PREFIX_LENGTH);
            length += PREFIX_LENGTH;
            Bytes.putBytes(errorMessageBytes, length, expectedValue, 0, expectedValue.length);
            length += expectedValue.length;
            Bytes.putBytes(errorMessageBytes, length, A_VALUE_PREFIX_BYTES, 0, PREFIX_LENGTH);
            length += PREFIX_LENGTH;
            Bytes.putBytes(errorMessageBytes, length, actualValue, 0, actualValue.length);

        } else {
            errorMessageBytes = Bytes.toBytes(errorMsg);
        }
        put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.ERROR_MESSAGE_BYTES, scanMaxTs, errorMessageBytes);
        if (isBeforeRebuild) {
            put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.VERIFICATION_PHASE_BYTES, scanMaxTs, PHASE_BEFORE_VALUE);
        } else {
            put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.VERIFICATION_PHASE_BYTES, scanMaxTs, PHASE_AFTER_VALUE);
        }
        outputHTable.put(put);
    }

    public void close() throws IOException {
        outputHTable.close();
    }
}
