package com.linkedin.pinot.common.utils;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Random;

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;


public class TestDataTableBuilder {
  @Test
  public void testSimple() throws Exception {
    final DataType[] columnTypes = DataType.values();
    final String[] columnNames = new String[columnTypes.length];
    final boolean[] isSingleValue = new boolean[columnTypes.length];

    for (int i = 0; i < columnTypes.length; i++) {
      columnNames[i] = columnTypes[i].toString();
    }
    final DataSchema schema = new DataSchema(columnNames, columnTypes, isSingleValue);

    final DataTableBuilder builder = new DataTableBuilder(schema);
    builder.open();
    final Random r = new Random();
    final int NUM_ROWS = 100;

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
    validate(schema, cArr, bArr, sArr, iArr, fArr, lArr, dArr, strArr, oArr, dataTable);
    final byte[] bytes = dataTable.toBytes();

    final DataTable newDataTable = new DataTable(bytes);
    validate(schema, cArr, bArr, sArr, iArr, fArr, lArr, dArr, strArr, oArr, newDataTable);

  }

  private void validate(DataSchema schema, char[] cArr, byte[] bArr, short[] sArr, int[] iArr, float[] fArr,
      long[] lArr, double[] dArr, String[] strArr, Object[] oArr, DataTable dataTable) {
    for (int rowId = 0; rowId < 1; rowId++) {
      for (int colId = 0; colId < schema.columnNames.length; colId++) {
        final DataType type = schema.columnTypes[colId];
        switch (type) {
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
