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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.IntFunction;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


@SuppressWarnings("rawtypes")
public class DataBlockBuilderTest {

  @DataProvider(name = "columnDataTypes")
  ColumnDataType[] columnDataTypes() {
    return EnumSet.complementOf(EnumSet.of(ColumnDataType.BYTES_ARRAY)).toArray(new ColumnDataType[0]);
  }

  @Test(dataProvider = "columnDataTypes")
  void testRowBlock(ColumnDataType type)
      throws IOException {
    int numRows = 100;
    List<Object[]> rows = generateRows(type, numRows);
    DataSchema dataSchema = new DataSchema(new String[]{"column"}, new ColumnDataType[]{type});
    AggregationFunction[] aggFunctions = null;
    if (type == ColumnDataType.OBJECT) {
      aggFunctions = new AggregationFunction[]{mock(AggregationFunction.class)};
    }
    DataBlock rowDataBlock = DataBlockBuilder.buildFromRows(rows, dataSchema, aggFunctions);
    assertEquals(rowDataBlock.getNumberOfRows(), numRows);
    checkEquals(type, rowDataBlock, i -> rows.get(i)[0]);
  }

  private List<Object[]> generateRows(ColumnDataType type, int numRows) {
    List<Object[]> result = new ArrayList<>();
    Random r = new Random(42);
    switch (type.getStoredType()) {
      case INT:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[]{r.nextInt()});
        }
        break;
      case LONG:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[]{r.nextLong()});
        }
        break;
      case FLOAT:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[]{r.nextFloat()});
        }
        break;
      case DOUBLE:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[]{r.nextDouble()});
        }
        break;
      case STRING:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[]{String.valueOf(r.nextInt())});
        }
        break;
      case BYTES:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[]{new ByteArray(String.valueOf(r.nextInt()).getBytes())});
        }
        break;
      case BIG_DECIMAL:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[]{BigDecimal.valueOf(r.nextInt())});
        }
        break;
      case MAP:
        for (int i = 0; i < numRows; i++) {
          Map<String, String> map = new HashMap<>();
          for (int j = 0; j < 10; j++) {
            map.put(String.valueOf(j), String.valueOf(r.nextInt()));
          }
          result.add(new Object[]{map});
        }
        break;
      case INT_ARRAY:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[]{new int[]{r.nextInt(), r.nextInt()}});
        }
        break;
      case LONG_ARRAY:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[]{new long[]{r.nextLong(), r.nextLong()}});
        }
        break;
      case FLOAT_ARRAY:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[]{new float[]{r.nextFloat(), r.nextFloat()}});
        }
        break;
      case DOUBLE_ARRAY:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[]{new double[]{r.nextDouble(), r.nextDouble()}});
        }
        break;
      case STRING_ARRAY:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[]{new String[]{String.valueOf(r.nextInt()), String.valueOf(r.nextInt())}});
        }
        break;
      case OBJECT:
      case UNKNOWN:
        for (int i = 0; i < numRows; i++) {
          result.add(new Object[1]);
        }
        break;
      default:
        throw new IllegalStateException("Unsupported data type: " + type);
    }
    for (int i = 0; i < numRows; i += 10) {
      result.set(i, new Object[]{null});
    }
    return result;
  }

  @Test(dataProvider = "columnDataTypes")
  void testColumnBlock(ColumnDataType type)
      throws IOException {
    int numRows = 100;
    Object[] column = generateColumns(type, numRows);
    DataSchema dataSchema = new DataSchema(new String[]{"column"}, new ColumnDataType[]{type});
    AggregationFunction[] aggFunctions = null;
    if (type == ColumnDataType.OBJECT) {
      aggFunctions = new AggregationFunction[]{mock(AggregationFunction.class)};
    }
    DataBlock rowDataBlock =
        DataBlockBuilder.buildFromColumns(Collections.singletonList(column), dataSchema, aggFunctions);
    assertEquals(rowDataBlock.getNumberOfRows(), numRows);
    checkEquals(type, rowDataBlock, i -> column[i]);
  }

  Object[] generateColumns(ColumnDataType type, int numRows) {
    Object[] result = new Object[numRows];
    Random r = new Random(42);
    switch (type.getStoredType()) {
      case INT:
        for (int i = 0; i < numRows; i++) {
          result[i] = r.nextInt();
        }
        break;
      case LONG:
        for (int i = 0; i < numRows; i++) {
          result[i] = r.nextLong();
        }
        break;
      case FLOAT:
        for (int i = 0; i < numRows; i++) {
          result[i] = r.nextFloat();
        }
        break;
      case DOUBLE:
        for (int i = 0; i < numRows; i++) {
          result[i] = r.nextDouble();
        }
        break;
      case STRING:
        for (int i = 0; i < numRows; i++) {
          result[i] = String.valueOf(r.nextInt());
        }
        break;
      case BYTES:
        for (int i = 0; i < numRows; i++) {
          result[i] = new ByteArray(String.valueOf(r.nextInt()).getBytes());
        }
        break;
      case MAP:
        for (int i = 0; i < numRows; i++) {
          result[i] = new HashMap<>();
          for (int j = 0; j < 10; j++) {
            ((HashMap) result[i]).put(String.valueOf(j), String.valueOf(r.nextInt()));
          }
        }
        break;
      case BIG_DECIMAL:
        for (int i = 0; i < numRows; i++) {
          result[i] = BigDecimal.valueOf(r.nextInt());
        }
        break;
      case INT_ARRAY:
        for (int i = 0; i < numRows; i++) {
          result[i] = new int[]{r.nextInt(), r.nextInt()};
        }
        break;
      case LONG_ARRAY:
        for (int i = 0; i < numRows; i++) {
          result[i] = new long[]{r.nextLong(), r.nextLong()};
        }
        break;
      case FLOAT_ARRAY:
        for (int i = 0; i < numRows; i++) {
          result[i] = new float[]{r.nextFloat(), r.nextFloat()};
        }
        break;
      case DOUBLE_ARRAY:
        for (int i = 0; i < numRows; i++) {
          result[i] = new double[]{r.nextDouble(), r.nextDouble()};
        }
        break;
      case STRING_ARRAY:
        for (int i = 0; i < numRows; i++) {
          result[i] = new String[]{String.valueOf(r.nextInt()), String.valueOf(r.nextInt())};
        }
        break;
      case OBJECT:
      case UNKNOWN:
        break;
      default:
        throw new IllegalStateException("Unsupported data type: " + type);
    }
    for (int i = 0; i < numRows; i += 10) {
      result[i] = null;
    }
    return result;
  }

  private void checkEquals(ColumnDataType type, DataBlock block, IntFunction<Object> rowToData) {
    int numRows = block.getNumberOfRows();
    switch (type.getStoredType()) {
      case INT:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getInt(i, 0), expected, "Failure on row " + i);
          }
        }
        break;
      case LONG:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getLong(i, 0), expected, "Failure on row " + i);
          }
        }
        break;
      case FLOAT:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getFloat(i, 0), expected, "Failure on row " + i);
          }
        }
        break;
      case DOUBLE:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getDouble(i, 0), expected, "Failure on row " + i);
          }
        }
        break;
      case STRING:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getString(i, 0), expected, "Failure on row " + i);
          }
        }
        break;
      case BYTES:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getBytes(i, 0), expected, "Failure on row " + i);
          }
        }
        break;
      case BIG_DECIMAL:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getBigDecimal(i, 0), expected, "Failure on row " + i);
          }
        }
        break;
      case MAP:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getMap(i, 0), expected, "Failure on row " + i);
          }
        }
        break;
      case INT_ARRAY:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getIntArray(i, 0), expected, "Failure on row " + i);
          }
        }
        break;
      case LONG_ARRAY:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getLongArray(i, 0), expected, "Failure on row " + i);
          }
        }
        break;
      case FLOAT_ARRAY:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getFloatArray(i, 0), expected, "Failure on row " + i);
          }
        }
        break;
      case DOUBLE_ARRAY:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getDoubleArray(i, 0), expected, "Failure on row " + i);
          }
        }
        break;
      case STRING_ARRAY:
        for (int i = 0; i < numRows; i++) {
          Object expected = rowToData.apply(i);
          if (expected != null) {
            assertEquals(block.getStringArray(i, 0), (String[]) rowToData.apply(i));
          }
        }
        break;
      case OBJECT:
      case UNKNOWN:
        for (int i = 0; i < numRows; i++) {
          assertNull(block.getCustomObject(i, 0));
        }
        break;
      default:
        throw new IllegalStateException("Unsupported data type: " + type);
    }
    if (type != ColumnDataType.OBJECT && type != ColumnDataType.UNKNOWN) {
      RoaringBitmap nullRowIds = block.getNullRowIds(0);

      BitSet actualBitSet = new BitSet(numRows);
      assert nullRowIds != null;
      nullRowIds.forEach((int i) -> actualBitSet.set(i));

      BitSet expectedBitSet = new BitSet(numRows);
      for (int i = 0; i < numRows; i++) {
        Object expectedValue = rowToData.apply(i);
        if (expectedValue == null) {
          expectedBitSet.set(i);
        }
      }
      Assert.assertEquals(actualBitSet, expectedBitSet, "Null row ids mismatch");
    }
  }
}
