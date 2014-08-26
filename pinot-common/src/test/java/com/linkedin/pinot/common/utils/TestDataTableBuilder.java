package com.linkedin.pinot.common.utils;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Random;

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataType;

public class TestDataTableBuilder {
	@Test
	public void testSimple() throws Exception {
		DataType[] columnTypes = DataType.values();
		String[] columnNames = new String[columnTypes.length];
		for (int i = 0; i < columnTypes.length; i++) {
			columnNames[i] = columnTypes[i].toString();
		}
		DataSchema schema = new DataSchema(columnNames, columnTypes);
		DataTableBuilder builder = new DataTableBuilder(schema);
		builder.open();
		Random r = new Random();
		int NUM_ROWS = 100;

		char[] cArr = new char[NUM_ROWS];
		byte[] bArr = new byte[NUM_ROWS];
		short[] sArr = new short[NUM_ROWS];
		int[] iArr = new int[NUM_ROWS];
		float[] fArr = new float[NUM_ROWS];
		long[] lArr = new long[NUM_ROWS];
		double[] dArr = new double[NUM_ROWS];
		String[] strArr = new String[NUM_ROWS];
		Object[] oArr = new Object[NUM_ROWS];

		for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
			builder.startRow();
			for (int colId = 0; colId < schema.columnNames.length; colId++) {
				DataType type = columnTypes[colId];
				switch (type) {
				case CHAR:
					char ch = (char) (r.nextInt(26) + 'a');
					cArr[rowId] = ch;
					builder.setColumn(colId, ch);
					break;
				case BYTE:
					byte b = (byte) (r.nextInt((int) Math.pow(2, 8)));
					bArr[rowId] = b;
					builder.setColumn(colId, b);

					break;
				case SHORT:
					short s = (short) (r.nextInt((int) Math.pow(2, 16)));
					sArr[rowId] = s;
					builder.setColumn(colId, s);

					break;
				case INT:
					int i = (int) (r.nextInt());
					iArr[rowId] = i;
					builder.setColumn(colId, i);

					break;
				case LONG:
					long l = (long) (r.nextLong());
					lArr[rowId] = l;
					builder.setColumn(colId, l);

					break;
				case FLOAT:
					float f = (float) (r.nextFloat());
					fArr[rowId] = f;
					builder.setColumn(colId, f);

					break;
				case DOUBLE:
					double d = (double) (r.nextDouble());
					dArr[rowId] = d;
					builder.setColumn(colId, d);
					break;
				case STRING:
					String str = new BigInteger(130, r).toString(32);
					strArr[rowId] = str;
					builder.setColumn(colId, str);
					break;
				case OBJECT:
					A obj = new A(r.nextInt());
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
		DataTable dataTable = builder.build();
		validate(schema, cArr, bArr, sArr, iArr, fArr, lArr, dArr, strArr,
				oArr, dataTable);
		byte[] bytes = dataTable.toBytes();
		
		DataTable newDataTable = new DataTable(bytes);
		validate(schema, cArr, bArr, sArr, iArr, fArr, lArr, dArr, strArr,
				oArr, newDataTable);
		
	}
	private void validate(DataSchema schema, char[] cArr, byte[] bArr,
			short[] sArr, int[] iArr, float[] fArr, long[] lArr, double[] dArr,
			String[] strArr, Object[] oArr, DataTable dataTable) {
		for (int rowId = 0; rowId < 1; rowId++) {
			for (int colId = 0; colId < schema.columnNames.length; colId++) {
				DataType type = schema.columnTypes[colId];
				switch (type) {
				case CHAR:
					Assert.assertEquals(cArr[rowId],
							dataTable.getChar(rowId, colId));
					break;
				case BYTE:
					Assert.assertEquals(bArr[rowId],
							dataTable.getByte(rowId, colId));
					break;
				case SHORT:
					Assert.assertEquals(sArr[rowId],
							dataTable.getShort(rowId, colId));
					break;
				case INT:
					Assert.assertEquals(iArr[rowId],
							dataTable.getInt(rowId, colId));
					break;
				case LONG:
					Assert.assertEquals(lArr[rowId],
							dataTable.getLong(rowId, colId));
					break;
				case FLOAT:
					Assert.assertEquals(fArr[rowId],
							dataTable.getFloat(rowId, colId));
					break;
				case DOUBLE:
					Assert.assertEquals(dArr[rowId],
							dataTable.getDouble(rowId, colId));
					break;
				case STRING:
					Assert.assertEquals(strArr[rowId],
							dataTable.getString(rowId, colId));
					break;
				case OBJECT:
					Assert.assertEquals(oArr[rowId],
							dataTable.getObject(rowId, colId));
					break;
				default:
					break;
				}
			}
		}
	}
	public static class A implements Serializable{
		final int i;
		
		public A(int val) {
			this.i = val;
		}
		

		@Override
		public boolean equals(Object obj) {
			return i == ((A)obj).i;
		}
		
		@Override
		public int hashCode() {
			return new Integer(i).hashCode();
		}
		
	}
}
