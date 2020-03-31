/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.mapreduce.index.IndexTool;

import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.REBUILT_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.RESULT_TABLE_COLUMN_FAMILY;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.SCANNED_DATA_ROW_COUNT_BYTES;

public class IndexToolVerificationResult {
    public static class PhaseResult {
        long validIndexRowCount = 0;
        long expiredIndexRowCount = 0;
        long missingIndexRowCount = 0;
        long invalidIndexRowCount = 0;

        public void add(PhaseResult phaseResult) {
            validIndexRowCount += phaseResult.validIndexRowCount;
            expiredIndexRowCount += phaseResult.expiredIndexRowCount;
            missingIndexRowCount += phaseResult.missingIndexRowCount;
            invalidIndexRowCount += phaseResult.invalidIndexRowCount;
        }

        public PhaseResult(){}

        public PhaseResult(long validIndexRowCount, long expiredIndexRowCount,
                long missingIndexRowCount, long invalidIndexRowCount) {
            this.validIndexRowCount = validIndexRowCount;
            this.expiredIndexRowCount = expiredIndexRowCount;
            this.missingIndexRowCount = missingIndexRowCount;
            this.invalidIndexRowCount = invalidIndexRowCount;
        }

        public long getTotalCount() {
            return validIndexRowCount + expiredIndexRowCount + missingIndexRowCount + invalidIndexRowCount;
        }

        @Override
        public String toString() {
            return "PhaseResult{" +
                    "validIndexRowCount=" + validIndexRowCount +
                    ", expiredIndexRowCount=" + expiredIndexRowCount +
                    ", missingIndexRowCount=" + missingIndexRowCount +
                    ", invalidIndexRowCount=" + invalidIndexRowCount +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            if (!(o instanceof PhaseResult)) {
                return false;
            }
            PhaseResult pr = (PhaseResult) o;
            return this.expiredIndexRowCount == pr.expiredIndexRowCount
                    && this.validIndexRowCount == pr.validIndexRowCount
                    && this.invalidIndexRowCount == pr.invalidIndexRowCount
                    && this.missingIndexRowCount == pr.missingIndexRowCount;
        }

        @Override
        public int hashCode() {
            long result = 17;
            result = 31 * result + expiredIndexRowCount;
            result = 31 * result + validIndexRowCount;
            result = 31 * result + missingIndexRowCount;
            result = 31 * result + invalidIndexRowCount;
            return (int)result;
        }
    }

    long scannedDataRowCount = 0;
    long rebuiltIndexRowCount = 0;
    PhaseResult before = new PhaseResult();
    PhaseResult after = new PhaseResult();

    @Override
    public String toString() {
        return "VerificationResult{" +
                "scannedDataRowCount=" + scannedDataRowCount +
                ", rebuiltIndexRowCount=" + rebuiltIndexRowCount +
                ", before=" + before +
                ", after=" + after +
                '}';
    }

    public long getScannedDataRowCount() {
        return scannedDataRowCount;
    }

    public long getRebuiltIndexRowCount() {
        return rebuiltIndexRowCount;
    }

    public long getBeforeRebuildValidIndexRowCount() {
        return before.validIndexRowCount;
    }

    public long getBeforeRebuildExpiredIndexRowCount() {
        return before.expiredIndexRowCount;
    }

    public long getBeforeRebuildInvalidIndexRowCount() {
        return before.invalidIndexRowCount;
    }

    public long getBeforeRebuildMissingIndexRowCount() {
        return before.missingIndexRowCount;
    }

    public long getAfterRebuildValidIndexRowCount() {
        return after.validIndexRowCount;
    }

    public long getAfterRebuildExpiredIndexRowCount() {
        return after.expiredIndexRowCount;
    }

    public long getAfterRebuildInvalidIndexRowCount() {
        return after.invalidIndexRowCount;
    }

    public long getAfterRebuildMissingIndexRowCount() {
        return after.missingIndexRowCount;
    }

    private void addScannedDataRowCount(long count) {
        this.scannedDataRowCount += count;
    }

    private void addRebuiltIndexRowCount(long count) {
        this.rebuiltIndexRowCount += count;
    }

    private void addBeforeRebuildValidIndexRowCount(long count) {
        before.validIndexRowCount += count;
    }

    private void addBeforeRebuildExpiredIndexRowCount(long count) {
        before.expiredIndexRowCount += count;
    }

    private void addBeforeRebuildMissingIndexRowCount(long count) {
        before.missingIndexRowCount += count;
    }

    private void addBeforeRebuildInvalidIndexRowCount(long count) {
        before.invalidIndexRowCount += count;
    }

    private void addAfterRebuildValidIndexRowCount(long count) {
        after.validIndexRowCount += count;
    }

    private void addAfterRebuildExpiredIndexRowCount(long count) {
        after.expiredIndexRowCount += count;
    }

    private void addAfterRebuildMissingIndexRowCount(long count) {
        after.missingIndexRowCount += count;
    }

    private void addAfterRebuildInvalidIndexRowCount(long count) {
        after.invalidIndexRowCount += count;
    }

    private static boolean isAfterRebuildInvalidIndexRowCount(Cell cell) {
        if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES, 0,
                AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES.length) == 0) {
            return true;
        }
        return false;
    }

    private long getValue(Cell cell) {
        return Long.parseLong(Bytes.toString(cell.getValueArray(),
                cell.getValueOffset(), cell.getValueLength()));
    }

    public void update(Cell cell) {
        if (CellUtil
                .matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, SCANNED_DATA_ROW_COUNT_BYTES)) {
            addScannedDataRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, REBUILT_INDEX_ROW_COUNT_BYTES)) {
            addRebuiltIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES)) {
            addBeforeRebuildValidIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES)) {
            addBeforeRebuildExpiredIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES)) {
            addBeforeRebuildMissingIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES)) {
            addBeforeRebuildInvalidIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildValidIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildExpiredIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildMissingIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildInvalidIndexRowCount(getValue(cell));
        }
    }

    public boolean isVerificationFailed(IndexTool.IndexVerifyType verifyType) {
        if (verifyType == IndexTool.IndexVerifyType.BEFORE || verifyType == IndexTool.IndexVerifyType.NONE) {
            return false;
        } else if (verifyType == IndexTool.IndexVerifyType.ONLY) {
            if (before.invalidIndexRowCount + before.missingIndexRowCount > 0) {
                return true;
            }
        } else if (verifyType == IndexTool.IndexVerifyType.BOTH || verifyType == IndexTool.IndexVerifyType.AFTER) {
            if (after.invalidIndexRowCount + after.missingIndexRowCount > 0) {
                return true;
            }
        }
        return false;
    }

    public void add(IndexToolVerificationResult verificationResult) {
        scannedDataRowCount += verificationResult.scannedDataRowCount;
        rebuiltIndexRowCount += verificationResult.rebuiltIndexRowCount;
        before.add(verificationResult.before);
        after.add(verificationResult.after);
    }
}
