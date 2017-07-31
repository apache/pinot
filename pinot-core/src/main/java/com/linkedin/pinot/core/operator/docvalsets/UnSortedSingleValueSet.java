/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.docvalsets;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BaseBlockValSet;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.metadata.column.ColumnMetadata;
import com.linkedin.pinot.core.operator.docvaliterators.UnSortedSingleValueIterator;


public final class UnSortedSingleValueSet extends BaseBlockValSet {
  final SingleColumnSingleValueReader sVReader;
  final ColumnMetadata columnMetadata;

  public UnSortedSingleValueSet(SingleColumnSingleValueReader sVReader, ColumnMetadata columnMetadata) {
    super();
    this.sVReader = sVReader;
    this.columnMetadata = columnMetadata;
  }

  @Override
  public BlockValIterator iterator() {
    return new UnSortedSingleValueIterator(sVReader, columnMetadata);
  }

  @Override
  public DataType getValueType() {
    return this.columnMetadata.getDataType();
  }

  /**
   * Reads int values for the given docIds and returns in the passed in double[].
   * Only 'int' data type allowed to be read in as 'int'.
   *
   * @param inDocIds DocIds for which to get the values
   * @param inStartPos start index in the inDocIds array
   * @param inDocIdsSize size of docIds to read
   * @param outValues Array where output is written
   * @param outStartPos start index into output array
   */
  @Override
  public void getIntValues(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = sVReader.createContext();

    if (columnMetadata.getDataType() == DataType.INT) {
      for (int i = inStartPos; i < inEndPos; i++) {
        outValues[outStartPos++] = sVReader.getInt(inDocIds[i], context);
      }
    } else {
      throw new UnsupportedOperationException(
          "Cannot fetch int values for column: " + columnMetadata.getColumnName());
    }
  }

  /**
   * Reads int values for the given docIds and returns in the passed in double[].
   * Compatible data types ('int' and 'long') can be read in as long.
   *
   * @param inDocIds DocIds for which to get the values
   * @param inStartPos start index in the inDocIds array
   * @param inDocIdsSize size of docIds to read
   * @param outValues Array where output is written
   * @param outStartPos start index into output array
   */
  @Override
  public void getLongValues(int[] inDocIds, int inStartPos, int inDocIdsSize, long[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = sVReader.createContext();

    switch (columnMetadata.getDataType()) {
      case INT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = sVReader.getInt(inDocIds[i], context);
        }
        break;

      case LONG:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = sVReader.getLong(inDocIds[i], context);
        }
        break;

      default:
        throw new UnsupportedOperationException(
            "Cannot fetch long values for column: " + columnMetadata.getColumnName());
    }
  }

  /**
   * Reads int values for the given docIds and returns in the passed in double[].
   * Compatible data types ('int', 'long' and 'float') can be read in as float.
   *
   * @param inDocIds DocIds for which to get the values
   * @param inStartPos start index in the inDocIds array
   * @param inDocIdsSize size of docIds to read
   * @param outValues Array where output is written
   * @param outStartPos start index into output array
   */
  @Override
  public void getFloatValues(int[] inDocIds, int inStartPos, int inDocIdsSize, float[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = sVReader.createContext();

    switch (columnMetadata.getDataType()) {
      case INT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = sVReader.getInt(inDocIds[i], context);
        }
        break;

      case LONG:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = sVReader.getLong(inDocIds[i], context);
        }
        break;

      case FLOAT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = sVReader.getFloat(inDocIds[i], context);
        }
        break;

      default:
        throw new UnsupportedOperationException(
            "Cannot fetch float values for column: " + columnMetadata.getColumnName());
    }
  }

  /**
   * Reads double values for the given docIds and returns in the passed in double[].
   * Compatible types (int, float, long) can also be read in as double.
   *
   * @param inDocIds DocIds for which to get the values
   * @param inStartPos start index in the inDocIds array
   * @param inDocIdsSize size of docIds to read
   * @param outValues Array where output is written
   * @param outStartPos start index into output array
   */
  @Override
  public void getDoubleValues(int[] inDocIds, int inStartPos, int inDocIdsSize, double[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = sVReader.createContext();

    switch (columnMetadata.getDataType()) {
      case INT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = sVReader.getInt(inDocIds[i], context);
        }
        break;

      case LONG:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = sVReader.getLong(inDocIds[i], context);
        }
        break;

      case FLOAT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = sVReader.getFloat(inDocIds[i], context);
        }
        break;

      case DOUBLE:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = sVReader.getDouble(inDocIds[i], context);
        }
        break;

      default:
        throw new UnsupportedOperationException(
            "Cannot fetch double values for column: " + columnMetadata.getColumnName());
    }
  }

  @Override
  public void getStringValues(int[] inDocIds, int inStartPos, int inDocIdsSize, String[] outValues, int outStartPos) {
    ReaderContext context = sVReader.createContext();
    int inEndPos = inStartPos + inDocIdsSize;
    try {
      for (int i = inStartPos; i < inEndPos; i++) {
        outValues[outStartPos++] = sVReader.getString(inDocIds[i], context);
      }
    } catch (Exception e) {
      Utils.rethrowException(e);
    }
  }

  @Override
  public void getDictionaryIds(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outDictionaryIds,
      int outStartPos) {
    sVReader.readValues(inDocIds, inStartPos, inDocIdsSize, outDictionaryIds, outStartPos);
  }
}
