/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.blocks;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.roaringbitmap.RoaringBatchIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * A lazy evalated data block, for not-yet fully resolved joins
 */
public class LazyDataBlock implements MseBlock.Data {
    private final DataBlock _leftBlock;
    private final DataSchema _resultSchema;
    private int _numRows;

    // Row-ids removed from the left block which should not be rendered
    @Nullable
    private RoaringBitmap _includedRowIds = null;

    @Nullable
    private List<Object[]> _rightRows = null;

    @Nullable
    private ColumnDataType[] _storedTypes = null;

    public LazyDataBlock(DataBlock leftBlock, DataSchema resultSchema) {
      _leftBlock = leftBlock;
      _resultSchema = resultSchema;
      _numRows = _leftBlock.getNumberOfRows();
    }

    @Override
    public int getNumRows() {
        return _numRows;
    }

    @Override
    public boolean isRowHeap() {
        return false;
    }

    @Override
    public RowHeapDataBlock asRowHeap() {
        return new RowHeapDataBlock(getRows(), _resultSchema);
    }

    @Override
    public SerializedDataBlock asSerialized() {
        // This is a bit annoying since we need to maintain several implementations based on the left side
        // First the easy case where we don't have to do anything (for example semi/anti joins which are no-ops)
        // if ((_rightRows == null || _rightRows.isEmpty()) && (_includedRowIds == null
        //    || _includedRowIds.getCardinality() == _leftBlock.getNumberOfRows())) {
        //    return new SerializedDataBlock(_leftBlock);
        //}

        /**
         DataBuffer fixedData = _leftBlock.getFixedData();
         DataBuffer varSizeData = _leftBlock.getVarSizeData();
         if (_leftBlock instanceof ColumnarDataBlock) {
         // Case 1: We are adding no columns and just removing rows
         // ColumnarDataBlock columnarDataBlock = (ColumnarDataBlock) _leftBlock;
         if (_rightRows == null || _rightRows.isEmpty()) {
         assert _includedRowIds != null;
         // TODO: Copy the column data from the original buffers, skipping removed rows
         // There's two main optimization opportunities, either we have a sparse block (most of the rows removed)
         if (!_includedRowIds.cardinalityExceeds(_numRows)) {
         // For the sparse block, we ought to just fully rebuild the data buffers
         // TODO: We can avoid all this unboxing, but I'm lazy and want to know if this is a wild goose-chase
         return asRowHeap().asSerialized();
         } else {
         // Or we have a dense block with a small number of rows removed
         // In this case it's more efficient to memcpy as much as we can

         // The sort of annoying part is that we have to keep track of dictionary items being removed
         }
         }

         // Case 2: We are adding columns, only
         if (_removedRowIds == null || _removedRowIds.isEmpty()) {
         // This is quite efficient => since the data is columnar already.
         }
         // Case 2: We are adding columns and removing rows
         }
         **/
        return asRowHeap().asSerialized();
    }

    @Override
    public DataSchema getDataSchema() {
        return _resultSchema;
    }

    @Override
    public <R, A> R accept(MseBlock.Data.Visitor<R, A> visitor, A arg) {
        return visitor.visit(this, arg);
    }

    private int getNumberOfColumns() {
        return _resultSchema.size();
    }

    public void setIncludedRowIds(RoaringBitmap includedRowIds) {
        if (includedRowIds == null || includedRowIds.getCardinality() == _leftBlock.getNumberOfRows()) {
            includedRowIds = null;
        }
        _includedRowIds = includedRowIds;
        if (_includedRowIds == null) {
            _numRows = _leftBlock.getNumberOfRows();
        } else {
            _numRows = _includedRowIds.getCardinality();
        }
    }

    public void setRightRows(List<Object[]> rightRows) {
        _rightRows = rightRows;
    }

    public Object[] getKeyColumns(int[] columnIds) {
        if (_storedTypes == null) {
            _storedTypes = _resultSchema.getStoredColumnDataTypes();
        }
        int numColumns = columnIds.length;
        Object[] values = new Object[numColumns];
        for (int i = 0; i < numColumns; i++) {
            switch (_storedTypes[columnIds[i]]) {
                case INT:
                    values[i] = DataBlockExtractUtils.extractIntColumn(
                        _storedTypes[columnIds[i]].toDataType(),
                        _leftBlock,
                        columnIds[i],
                        _leftBlock.getNullRowIds(columnIds[i])
                    );
                    break;
                case LONG:
                    values[i] = DataBlockExtractUtils.extractLongColumn(
                        _storedTypes[columnIds[i]].toDataType(),
                        _leftBlock,
                        columnIds[i],
                        _leftBlock.getNullRowIds(columnIds[i])
                    );
                    break;
                case FLOAT:
                    values[i] = DataBlockExtractUtils.extractFloatColumn(
                        _storedTypes[columnIds[i]].toDataType(),
                        _leftBlock,
                        columnIds[i],
                        _leftBlock.getNullRowIds(columnIds[i])
                    );
                    break;
                case DOUBLE:
                    values[i] = DataBlockExtractUtils.extractDoubleColumn(
                        _storedTypes[columnIds[i]].toDataType(),
                        _leftBlock,
                        columnIds[i],
                        _leftBlock.getNullRowIds(columnIds[i])
                    );
                    break;
                case BIG_DECIMAL:
                    values[i] = DataBlockExtractUtils.extractBigDecimalColumn(
                        _storedTypes[columnIds[i]].toDataType(),
                        _leftBlock,
                        columnIds[i],
                        _leftBlock.getNullRowIds(columnIds[i])
                    );
                    break;
                case STRING:
                    String[] stringValues = DataBlockExtractUtils.extractStringColumn(
                        _storedTypes[columnIds[i]].toDataType(),
                        _leftBlock,
                        columnIds[i],
                        _leftBlock.getNullRowIds(columnIds[i])
                    );
                    values[i] = stringValues;
                    break;
                case BYTES:
                    values[i] = DataBlockExtractUtils.extractBytesColumn(
                        _storedTypes[columnIds[i]].toDataType(),
                        _leftBlock,
                        columnIds[i],
                        _leftBlock.getNullRowIds(columnIds[i])
                    );
                    break;
                default:
                    throw new IllegalStateException(
                        "Unsupported stored type: " + _storedTypes[columnIds[i]] + " for column: "
                            + _resultSchema.getColumnName(columnIds[i])
                    );
            }
        }
        return values;
    }

    public List<Object[]> getRows() {
        if (_storedTypes == null) {
            _storedTypes = _resultSchema.getStoredColumnDataTypes();
        }
        int leftNumColumns = _leftBlock.getNumberOfColumns();
        int totalNumColumns = getNumberOfColumns();
        RoaringBitmap[] nullBitmaps = new RoaringBitmap[leftNumColumns];
        for (int colId = 0; colId < leftNumColumns; colId++) {
            nullBitmaps[colId] = _leftBlock.getNullRowIds(colId);
        }
        List<Object[]> rows = new ArrayList<>(_numRows);
        if (_includedRowIds == null) {
            for (int rowId = 0; rowId < _numRows; rowId++) {
                Object[] row = new Object[totalNumColumns];
                for (int colId = 0; colId < leftNumColumns; colId++) {
                    RoaringBitmap nullBitmap = nullBitmaps[colId];
                    if (nullBitmap == null || !nullBitmap.contains(rowId)) {
                        row[colId] = DataBlockExtractUtils.extractValue(_leftBlock, _storedTypes[colId], rowId, colId);
                    }
                }
                if (_rightRows != null) {
                    Object[] rightRow = _rightRows.get(rowId);
                    if (rightRow != null) {
                        System.arraycopy(rightRow, 0, row, leftNumColumns, rightRow.length);
                    }
                }
                rows.add(row);
            }
        } else {
            int[] buffer = new int[256];
            RoaringBatchIterator it = _includedRowIds.getBatchIterator();
            while (it.hasNext()) {
                int batchSize = it.nextBatch(buffer);
                for (int i = 0; i < batchSize; i++) {
                    int rowId = buffer[i];
                    Object[] row = new Object[totalNumColumns];
                    for (int colId = 0; colId < leftNumColumns; colId++) {
                        RoaringBitmap nullBitmap = nullBitmaps[colId];
                        if (nullBitmap == null || !nullBitmap.contains(rowId)) {
                            row[colId] =
                                DataBlockExtractUtils.extractValue(_leftBlock, _storedTypes[colId], rowId, colId);
                        }
                    }
                    if (_rightRows != null) {
                        Object[] rightRow = _rightRows.get(rowId);
                        if (rightRow != null) {
                            System.arraycopy(rightRow, 0, row, leftNumColumns, rightRow.length);
                        }
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }
}
