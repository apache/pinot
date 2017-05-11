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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BaseBlockValSet;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.operator.docvaliterators.RealtimeSingleValueIterator;


public class RealtimeFixedWidthRawValueSet extends BaseBlockValSet {
  private final FixedByteSingleColumnSingleValueReaderWriter reader;
  private final int length;
  private final FieldSpec.DataType dataType;
  private final String columnName;

  public RealtimeFixedWidthRawValueSet(FixedByteSingleColumnSingleValueReaderWriter reader, int length,
      FieldSpec.DataType dataType, String columnName) {
    super();
    this.reader = reader;
    this.length = length;
    this.dataType = dataType;
    this.columnName = columnName;
  }

  @Override
  public BlockValIterator iterator() {
    // throw unimplemented?
    return new RealtimeSingleValueIterator(reader, length, dataType);
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return dataType;
  }

  @Override
  public void getIntValues(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outValues, int outStartPos) {
    if (dataType.equals(FieldSpec.DataType.INT)) {
      reader.readValues(inDocIds, inStartPos, inDocIdsSize, outValues, outStartPos);
    } else {
      throw new UnsupportedOperationException("Cannot fetch int values for column: " + columnName);
    }
  }

  @Override
  public void getLongValues(int[] inDocIds, int inStartPos, int inDocIdsSize, long[] outValues, int outStartPos) {
    int endPos = inStartPos + inDocIdsSize;
    int outPos = outStartPos;
    switch (dataType) {
      case INT:
        for (int inPos = inStartPos; inPos < endPos; inPos++) {
          outValues[outPos++] = reader.getInt(inDocIds[inPos]);
        }
        break;
      case LONG:
        reader.readValues(inDocIds, inStartPos, inDocIdsSize, outValues, outStartPos);
        break;
      default:
        throw new UnsupportedOperationException("Cannot fetch long values for column: " + columnName);
    }
  }

  @Override
  public void getFloatValues(int[] inDocIds, int inStartPos, int inDocIdsSize, float[] outValues, int outStartPos) {
    int endPos = inStartPos + inDocIdsSize;
    int outPos = outStartPos;
    switch (dataType) {
      case INT:
        for (int inPos = inStartPos; inPos < endPos; inPos++) {
          outValues[outPos++] = reader.getInt(inDocIds[inPos]);
        }
        break;
      case LONG:
        for (int inPos = inStartPos; inPos < endPos; inPos++) {
          outValues[outPos++] = reader.getLong(inDocIds[inPos]);
        }
        break;
      case FLOAT:
        reader.readValues(inDocIds, inStartPos, inDocIdsSize, outValues, outStartPos);
        break;
      default:
        throw new UnsupportedOperationException("Cannot fetch float values for column: " + columnName);
    }
  }

  @Override
  public void getDoubleValues(int[] inDocIds, int inStartPos, int inDocIdsSize, double[] outValues, int outStartPos) {
    int endPos = inStartPos + inDocIdsSize;
    int outPos = outStartPos;
    switch (dataType) {
      case INT:
        for (int inPos = inStartPos; inPos < endPos; inPos++) {
          outValues[outPos++] = reader.getInt(inDocIds[inPos]);
        }
        break;
      case LONG:
        for (int inPos = inStartPos; inPos < endPos; inPos++) {
          outValues[outPos++] = reader.getLong(inDocIds[inPos]);
        }
        break;
      case FLOAT:
        for (int inPos = inStartPos; inPos < endPos; inPos++) {
          outValues[outPos++] = reader.getFloat(inDocIds[inPos]);
        }
        break;
      case DOUBLE:
        reader.readValues(inDocIds, inStartPos, inDocIdsSize, outValues, outStartPos);
        break;
      default:
        throw new UnsupportedOperationException("Cannot fetch double values for column: " + columnName);
    }
  }
}
