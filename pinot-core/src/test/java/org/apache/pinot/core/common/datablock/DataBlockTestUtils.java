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
package org.apache.pinot.core.common.datablock;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


public class DataBlockTestUtils {
  private static final Random RANDOM = new Random();
  private static final int ARRAY_SIZE = 5;

  private DataBlockTestUtils() {
    // do not instantiate.
  }

  public static Object[] getRandomRow(DataSchema dataSchema, int nullPercentile) {
    final int numColumns = dataSchema.getColumnNames().length;
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    Object[] row = new Object[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      switch (columnDataTypes[colId]) {
        case INT:
          row[colId] = RANDOM.nextInt();
          break;
        case LONG:
          row[colId] = RANDOM.nextLong();
          break;
        case FLOAT:
          row[colId] = RANDOM.nextFloat();
          break;
        case DOUBLE:
          row[colId] = RANDOM.nextDouble();
          break;
        case BIG_DECIMAL:
          row[colId] = BigDecimal.valueOf(RANDOM.nextDouble());
          break;
        case BOOLEAN:
          row[colId] = RANDOM.nextBoolean() ? 1 : 0;
          break;
        case TIMESTAMP:
          row[colId] = RANDOM.nextLong();
          break;
        case STRING:
          row[colId] = RandomStringUtils.random(RANDOM.nextInt(20));
          break;
        case BYTES:
          row[colId] = new ByteArray(RandomStringUtils.random(RANDOM.nextInt(20)).getBytes());
          break;
        case INT_ARRAY:
          int length = RANDOM.nextInt(ARRAY_SIZE);
          int[] intArray = new int[length];
          for (int i = 0; i < length; i++) {
            intArray[i] = RANDOM.nextInt();
          }
          row[colId] = intArray;
          break;
        case LONG_ARRAY:
          length = RANDOM.nextInt(ARRAY_SIZE);
          long[] longArray = new long[length];
          for (int i = 0; i < length; i++) {
            longArray[i] = RANDOM.nextLong();
          }
          row[colId] = longArray;
          break;
        case FLOAT_ARRAY:
          length = RANDOM.nextInt(ARRAY_SIZE);
          float[] floatArray = new float[length];
          for (int i = 0; i < length; i++) {
            floatArray[i] = RANDOM.nextFloat();
          }
          row[colId] = floatArray;
          break;
        case DOUBLE_ARRAY:
          length = RANDOM.nextInt(ARRAY_SIZE);
          double[] doubleArray = new double[length];
          for (int i = 0; i < length; i++) {
            doubleArray[i] = RANDOM.nextDouble();
          }
          row[colId] = doubleArray;
          break;
        case STRING_ARRAY:
          length = RANDOM.nextInt(ARRAY_SIZE);
          String[] stringArray = new String[length];
          for (int i = 0; i < length; i++) {
            stringArray[i] = RandomStringUtils.random(RANDOM.nextInt(20));
          }
          row[colId] = stringArray;
          break;
        case BOOLEAN_ARRAY:
          length = RANDOM.nextInt(ARRAY_SIZE);
          int[] booleanArray = new int[length];
          for (int i = 0; i < length; i++) {
            booleanArray[i] = RANDOM.nextBoolean() ? 1 : 0;
          }
          row[colId] = booleanArray;
          break;
        case TIMESTAMP_ARRAY:
          length = RANDOM.nextInt(ARRAY_SIZE);
          long[] timestampArray = new long[length];
          for (int i = 0; i < length; i++) {
            timestampArray[i] = RANDOM.nextLong();
          }
          row[colId] = timestampArray;
          break;
        case UNKNOWN:
          row[colId] = null;
          break;
        default:
          throw new UnsupportedOperationException("Can't fill random data for column type: " + columnDataTypes[colId]);
      }
      // randomly set some entry to null
      if (columnDataTypes[colId].getStoredType() != ColumnDataType.OBJECT) {
        row[colId] = randomlySettingNull(nullPercentile) ? null : row[colId];
      }
    }
    return row;
  }

  public static Object getElement(DataBlock dataBlock, int rowId, int colId, ColumnDataType columnDataType) {
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    if (nullBitmap != null) {
      if (nullBitmap.contains(rowId)) {
        return null;
      }
    }
    switch (columnDataType.getStoredType()) {
      case INT:
        return dataBlock.getInt(rowId, colId);
      case LONG:
        return dataBlock.getLong(rowId, colId);
      case FLOAT:
        return dataBlock.getFloat(rowId, colId);
      case DOUBLE:
        return dataBlock.getDouble(rowId, colId);
      case BIG_DECIMAL:
        return dataBlock.getBigDecimal(rowId, colId);
      case STRING:
        return dataBlock.getString(rowId, colId);
      case BYTES:
        return dataBlock.getBytes(rowId, colId);
      case INT_ARRAY:
        return dataBlock.getIntArray(rowId, colId);
      case LONG_ARRAY:
        return dataBlock.getLongArray(rowId, colId);
      case FLOAT_ARRAY:
        return dataBlock.getFloatArray(rowId, colId);
      case DOUBLE_ARRAY:
        return dataBlock.getDoubleArray(rowId, colId);
      case STRING_ARRAY:
        return dataBlock.getStringArray(rowId, colId);
      case UNKNOWN:
        return null;
      default:
        throw new UnsupportedOperationException("Can't retrieve data for column type: " + columnDataType);
    }
  }

  public static List<Object[]> getRandomRows(DataSchema dataSchema, int numRows, int nullPercentile) {
    List<Object[]> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      rows.add(getRandomRow(dataSchema, nullPercentile));
    }
    return rows;
  }

  public static List<Object[]> convertColumnar(DataSchema dataSchema, List<Object[]> rows) {
    final int numRows = rows.size();
    final int numColumns = dataSchema.getColumnNames().length;
    List<Object[]> columnars = new ArrayList<>(numColumns);
    for (int colId = 0; colId < numColumns; colId++) {
      columnars.add(new Object[numRows]);
      for (int rowId = 0; rowId < numRows; rowId++) {
        columnars.get(colId)[rowId] = rows.get(rowId)[colId];
      }
    }
    return columnars;
  }

  public static boolean randomlySettingNull(int percentile) {
    return RANDOM.nextInt(100) >= (100 - percentile);
  }
}
