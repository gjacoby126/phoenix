package org.apache.phoenix.index;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.mapreduce.index.IndexTool;

import java.io.IOException;

import static org.apache.phoenix.mapreduce.index.IndexTool.AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.REBUILT_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.RESULT_TABLE_COLUMN_FAMILY;
import static org.apache.phoenix.mapreduce.index.IndexTool.SCANNED_DATA_ROW_COUNT_BYTES;

public class IndexVerificationResultRepository {
    private Table resultHTable;
    private Table indexHTable;
    public static final byte[] ROW_KEY_SEPARATOR_BYTE = Bytes.toBytes("|");

    public IndexVerificationResultRepository(byte[] indexName,
                                             HTableFactory hTableFactory) throws IOException {
        resultHTable = hTableFactory.getTable(new ImmutableBytesPtr(IndexTool.RESULT_TABLE_NAME_BYTES));
        indexHTable = hTableFactory.getTable(new ImmutableBytesPtr(indexName));
    }

    public static byte[] generateResultTableRowKey(long ts, byte[] indexTableName,  byte [] regionName,
                                                    byte[] startRow, byte[] stopRow) {
        byte[] keyPrefix = Bytes.toBytes(Long.toString(ts));
        int targetOffset = 0;
        // The row key for the result table : timestamp | index table name | datable table region name |
        //                                    scan start row | scan stop row
        byte[] rowKey = new byte[keyPrefix.length + ROW_KEY_SEPARATOR_BYTE.length + indexTableName.length +
            ROW_KEY_SEPARATOR_BYTE.length + regionName.length + ROW_KEY_SEPARATOR_BYTE.length +
            startRow.length + ROW_KEY_SEPARATOR_BYTE.length + stopRow.length];
        Bytes.putBytes(rowKey, targetOffset, keyPrefix, 0, keyPrefix.length);
        targetOffset += keyPrefix.length;
        Bytes.putBytes(rowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(rowKey, targetOffset, indexTableName, 0, indexTableName.length);
        targetOffset += indexTableName.length;
        Bytes.putBytes(rowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(rowKey, targetOffset, regionName, 0, regionName.length);
        targetOffset += regionName.length;
        Bytes.putBytes(rowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(rowKey, targetOffset, startRow, 0, startRow.length);
        targetOffset += startRow.length;
        Bytes.putBytes(rowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(rowKey, targetOffset, stopRow, 0, stopRow.length);
        return rowKey;
    }

    public void logToIndexToolResultTable(IndexToolVerificationResult verificationResult,
                                          Scan scan, IndexTool.IndexVerifyType verifyType,
                                          byte[] region) throws IOException {
        long scanMaxTs = scan.getTimeRange().getMax();
        byte[] rowKey = generateResultTableRowKey(scanMaxTs, indexHTable.getName().toBytes(),
            region, scan.getStartRow(), scan.getStopRow());
        Put put = new Put(rowKey);
        put.addColumn(RESULT_TABLE_COLUMN_FAMILY, SCANNED_DATA_ROW_COUNT_BYTES,
            scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.getScannedDataRowCount())));
        put.addColumn(RESULT_TABLE_COLUMN_FAMILY, REBUILT_INDEX_ROW_COUNT_BYTES,
            scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.getRebuiltIndexRowCount())));
        if (verifyType == IndexTool.IndexVerifyType.BEFORE || verifyType == IndexTool.IndexVerifyType.BOTH ||
            verifyType == IndexTool.IndexVerifyType.ONLY) {
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES,
                scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.getBeforeRebuildValidIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES,
                scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.getBeforeRebuildExpiredIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES,
                scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.getBeforeRebuildMissingIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES,
                scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.getBeforeRebuildInvalidIndexRowCount())));
        }
        if (verifyType == IndexTool.IndexVerifyType.AFTER || verifyType == IndexTool.IndexVerifyType.BOTH) {
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES,
                scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.getAfterRebuildValidIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES,
                scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.getAfterRebuildExpiredIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES,
                scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.getAfterRebuildMissingIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES,
                scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.getAfterRebuildInvalidIndexRowCount())));
        }
        resultHTable.put(put);
    }

    public void close() throws IOException {
        resultHTable.close();
    }
}

