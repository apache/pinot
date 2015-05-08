/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.utils;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;


public class TestDataTableBuilder {

  @Test
  public void testException() throws Exception {
    Exception exception = new UnsupportedOperationException("msg 0");
    ProcessingException processingException = QueryException.EXECUTION_TIMEOUT_ERROR.deepCopy();
    processingException.setMessage(exception.toString());
    DataTable dataTable = new DataTable();
    dataTable.addException(processingException);
    byte[] bytes = dataTable.toBytes();
    DataTable desDataTable = new DataTable(bytes);
    String exceptionMsg = desDataTable.getMetadata().get("Exception" + QueryException.EXECUTION_TIMEOUT_ERROR.getErrorCode());
    org.testng.Assert.assertEquals(exceptionMsg, exception.toString());
  }

  @Test
  public void testSimple() throws Exception {
    final DataType[] columnTypes = DataType.values();
    final String[] columnNames = new String[columnTypes.length];

    for (int i = 0; i < columnTypes.length; i++) {
      columnNames[i] = columnTypes[i].toString();
    }
    final DataSchema schema = new DataSchema(columnNames, columnTypes);

    final DataTableBuilder builder = new DataTableBuilder(schema);
    builder.open();
    final Random r = new Random();
    final int NUM_ROWS = 100;

    final boolean[] boolArr = new boolean[NUM_ROWS];
    final char[] cArr = new char[NUM_ROWS];
    final byte[] bArr = new byte[NUM_ROWS];
    final short[] sArr = new short[NUM_ROWS];
    final int[] iArr = new int[NUM_ROWS];
    final float[] fArr = new float[NUM_ROWS];
    final long[] lArr = new long[NUM_ROWS];
    final double[] dArr = new double[NUM_ROWS];
    final String[] strArr = new String[NUM_ROWS];
    final Object[] oArr = new Object[NUM_ROWS];

    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      builder.startRow();
      for (int colId = 0; colId < schema.columnNames.length; colId++) {
        final DataType type = columnTypes[colId];
        switch (type) {
          case BOOLEAN:
            final boolean bool = r.nextBoolean();
            boolArr[rowId] = bool;
            builder.setColumn(colId, bool);
            break;
          case CHAR:
            final char ch = (char) (r.nextInt(26) + 'a');
            cArr[rowId] = ch;
            builder.setColumn(colId, ch);
            break;
          case BYTE:
            final byte b = (byte) (r.nextInt((int) Math.pow(2, 8)));
            bArr[rowId] = b;
            builder.setColumn(colId, b);

            break;
          case SHORT:
            final short s = (short) (r.nextInt((int) Math.pow(2, 16)));
            sArr[rowId] = s;
            builder.setColumn(colId, s);

            break;
          case INT:
            final int i = (r.nextInt());
            iArr[rowId] = i;
            builder.setColumn(colId, i);

            break;
          case LONG:
            final long l = (r.nextLong());
            lArr[rowId] = l;
            builder.setColumn(colId, l);

            break;
          case FLOAT:
            final float f = (r.nextFloat());
            fArr[rowId] = f;
            builder.setColumn(colId, f);
            break;
          case DOUBLE:
            final double d = (r.nextDouble());
            dArr[rowId] = d;
            builder.setColumn(colId, d);
            break;
          case STRING:
            final String str = new BigInteger(130, r).toString(32);
            strArr[rowId] = str;
            builder.setColumn(colId, str);
            break;
          case OBJECT:
            final A obj = new A(r.nextInt());
            oArr[rowId] = obj;
            builder.setColumn(colId, obj);

            break;
          default:
            break;
        }
      }
      builder.finishRow();
    }
    builder.seal();
    final DataTable dataTable = builder.build();
    //System.out.println(dataTable);
    validate(dataTable, NUM_ROWS, schema, boolArr, cArr, bArr, sArr, iArr,
        fArr, lArr, dArr, strArr, oArr);
    final byte[] bytes = dataTable.toBytes();

    final DataTable newDataTable = new DataTable(bytes);
    validate(newDataTable, NUM_ROWS, schema, boolArr, cArr, bArr, sArr, iArr,
        fArr, lArr, dArr, strArr, oArr);

  }

  @Test
  public void testStringArray() throws Exception {
    DataType[] columnTypes = new DataType[] { DataType.STRING_ARRAY };
    String[] columnNames = new String[] { "col-0" };
    DataSchema schema = new DataSchema(columnNames, columnTypes);
    DataTableBuilder builder = new DataTableBuilder(schema);
    builder.open();
    Random r = new Random();
    int NUM_ROWS = 10;
    Object[] oStringArray = new Object[NUM_ROWS];
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      builder.startRow();
      int size = r.nextInt(15);
      String[] arr = new String[size];
      for (int j = 0; j < size; j++) {
        arr[j] = new BigInteger(130, r).toString(32);
      }
      oStringArray[rowId] = arr;
      builder.setColumn(0, arr);
      builder.finishRow();
    }
    builder.seal();
    DataTable dataTable = builder.build();
    System.out.println(dataTable.toString());
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      validate(DataType.STRING_ARRAY, dataTable, oStringArray, rowId, 0);
    }

    byte[] bytes = dataTable.toBytes();
    DataTable newDataTable = new DataTable(bytes);
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      validate(DataType.STRING_ARRAY, newDataTable, oStringArray, rowId, 0);
    }

  }

  @Test
  public void testIntArray() throws Exception {
    DataType[] columnTypes = new DataType[] { DataType.INT_ARRAY };
    String[] columnNames = new String[] { "col-0" };
    DataSchema schema = new DataSchema(columnNames, columnTypes);
    DataTableBuilder builder = new DataTableBuilder(schema);
    builder.open();
    Random r = new Random();
    int NUM_ROWS = 10;
    Object[] oIntArr1 = new Object[NUM_ROWS];
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      builder.startRow();
      int size1 = r.nextInt(15);
      int[] arr1 = new int[size1];
      for (int j = 0; j < size1; j++) {
        arr1[j] = r.nextInt();
      }
      oIntArr1[rowId] = arr1;
      builder.setColumn(0, arr1);
      builder.finishRow();
    }
    builder.seal();
    DataTable dataTable = builder.build();
    System.out.println(dataTable.toString());
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      validate(DataType.INT_ARRAY, dataTable, oIntArr1, rowId, 0);
    }

    byte[] bytes = dataTable.toBytes();

    DataTable newDataTable = new DataTable(bytes);
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      validate(DataType.INT_ARRAY, newDataTable, oIntArr1, rowId, 0);
    }

  }

  @Test
  public void testComplexDataTypes() throws Exception {
    DataType[] columnTypes = new DataType[] { DataType.INT_ARRAY,
        DataType.INT_ARRAY };
    String[] columnNames = new String[] { "col-0", "col-1" };
    DataSchema schema = new DataSchema(columnNames, columnTypes);
    DataTableBuilder builder = new DataTableBuilder(schema);
    builder.open();
    Random r = new Random();
    int NUM_ROWS = 100;
    Object[] oIntArr1 = new Object[NUM_ROWS];
    Object[] oIntArr2 = new Object[NUM_ROWS];
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      builder.startRow();
      int size1 = r.nextInt(15);
      int size2 = r.nextInt(15);
      int[] arr1 = new int[size1];
      int[] arr2 = new int[size2];
      for (int j = 0; j < size1; j++) {
        arr1[j] = r.nextInt();
      }
      for (int j = 0; j < size2; j++) {
        arr2[j] = r.nextInt();
      }

      oIntArr1[rowId] = arr1;
      oIntArr2[rowId] = arr2;
      builder.setColumn(0, arr1);
      builder.setColumn(1, arr2);
      builder.finishRow();
    }
    builder.seal();
    DataTable dataTable = builder.build();
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      validate(DataType.INT_ARRAY, dataTable, oIntArr1, rowId, 0);
      validate(DataType.INT_ARRAY, dataTable, oIntArr2, rowId, 1);
    }

    byte[] bytes = dataTable.toBytes();

    DataTable newDataTable = new DataTable(bytes);
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      validate(DataType.INT_ARRAY, newDataTable, oIntArr1, rowId, 0);
      validate(DataType.INT_ARRAY, newDataTable, oIntArr2, rowId, 1);
    }

  }

  private void validate(DataType type, DataTable dataTable, Object[] arr,
      int rowId, int colId) {
    switch (type) {
      case BOOLEAN:
        Assert.assertEquals(arr[rowId], dataTable.getBoolean(rowId, colId));
        break;
      case CHAR:
        Assert.assertEquals(arr[rowId], dataTable.getChar(rowId, colId));
        break;
      case BYTE:
        Assert.assertEquals(arr[rowId], dataTable.getByte(rowId, colId));
        break;
      case SHORT:
        Assert.assertEquals(arr[rowId], dataTable.getShort(rowId, colId));
        break;
      case INT:
        Assert.assertEquals(arr[rowId], dataTable.getInt(rowId, colId));
        break;
      case LONG:
        Assert.assertEquals(arr[rowId], dataTable.getLong(rowId, colId));
        break;
      case FLOAT:
        Assert.assertEquals(arr[rowId], dataTable.getFloat(rowId, colId));
        break;
      case DOUBLE:
        Assert.assertEquals(arr[rowId], dataTable.getDouble(rowId, colId));
        break;
      case STRING:
        Assert.assertEquals(arr[rowId], dataTable.getString(rowId, colId));
        break;
      case BYTE_ARRAY:
        byte[] expectedByteArray = (byte[]) arr[rowId];
        byte[] actualByteArray = (byte[]) dataTable.getByteArray(rowId, colId);
        Assert.assertEquals(expectedByteArray.length, actualByteArray.length);
        Assert.assertTrue(Arrays.equals(expectedByteArray, actualByteArray));
        break;
      case CHAR_ARRAY:
        char[] expectedCharArray = (char[]) arr[rowId];
        char[] actualChartArray = (char[]) dataTable.getCharArray(rowId, colId);
        Assert.assertEquals(expectedCharArray.length, actualChartArray.length);
        Assert.assertTrue(Arrays.equals(expectedCharArray, actualChartArray));
        break;
      case INT_ARRAY:
        int[] expectedIntArray = (int[]) arr[rowId];
        int[] actualIntArray = (int[]) dataTable.getIntArray(rowId, colId);
        Assert.assertEquals(expectedIntArray.length, actualIntArray.length);
        Assert.assertTrue(Arrays.equals(expectedIntArray, actualIntArray));
        break;
      case STRING_ARRAY:
        String[] expectedStringArray = (String[]) arr[rowId];
        String[] actualStringArray = (String[]) dataTable.getStringArray(rowId, colId);
        Assert.assertEquals(expectedStringArray.length, actualStringArray.length);
        Assert.assertTrue(Arrays.equals(expectedStringArray, actualStringArray));
        break;
      case OBJECT:
        Assert.assertEquals(arr[rowId], dataTable.getObject(rowId, colId));
        break;
      default:
        break;
    }
  }

  private void validate(DataTable dataTable, int numRows, DataSchema schema,
      boolean[] boolArr, char[] cArr, byte[] bArr, short[] sArr, int[] iArr,
      float[] fArr, long[] lArr, double[] dArr, String[] strArr, Object[] oArr) {
    for (int rowId = 0; rowId < numRows; rowId++) {
      for (int colId = 0; colId < schema.columnNames.length; colId++) {
        final DataType type = schema.columnTypes[colId];
        switch (type) {
          case BOOLEAN:
            Assert.assertEquals(boolArr[rowId],
                dataTable.getBoolean(rowId, colId));
            break;
          case CHAR:
            Assert.assertEquals(cArr[rowId], dataTable.getChar(rowId, colId));
            break;
          case BYTE:
            Assert.assertEquals(bArr[rowId], dataTable.getByte(rowId, colId));
            break;
          case SHORT:
            Assert.assertEquals(sArr[rowId], dataTable.getShort(rowId, colId));
            break;
          case INT:
            Assert.assertEquals(iArr[rowId], dataTable.getInt(rowId, colId));
            break;
          case LONG:
            Assert.assertEquals(lArr[rowId], dataTable.getLong(rowId, colId));
            break;
          case FLOAT:
            Assert.assertEquals(fArr[rowId], dataTable.getFloat(rowId, colId));
            break;
          case DOUBLE:
            Assert.assertEquals(dArr[rowId], dataTable.getDouble(rowId, colId));
            break;
          case STRING:
            Assert.assertEquals(strArr[rowId], dataTable.getString(rowId, colId));
            break;
          case OBJECT:
            Assert.assertEquals(oArr[rowId], dataTable.getObject(rowId, colId));
            break;
          default:
            break;
        }
      }
    }
  }

  public static class A implements Serializable {
    final int i;

    public A(int val) {
      i = val;
    }

    @Override
    public boolean equals(Object obj) {
      return i == ((A) obj).i;
    }

    @Override
    public int hashCode() {
      return new Integer(i).hashCode();
    }
  }
}
