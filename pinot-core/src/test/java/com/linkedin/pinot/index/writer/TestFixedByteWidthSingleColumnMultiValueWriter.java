package com.linkedin.pinot.index.writer;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.index.writer.impl.FixedByteWidthSingleColumnMultiValueWriter;

public class TestFixedByteWidthSingleColumnMultiValueWriter {
	@Test
	public void testSingleCol() throws Exception {

		File file = new File("test_single_col_writer.dat");
		file.delete();
		int rows = 100;
		int[][] data = new int[rows][];
		Random r = new Random();
		int totalNumValues = 0;
		for (int i = 0; i < rows; i++) {
			int numValues = r.nextInt(100) + 1;
			data[i] = new int[numValues];
			for (int j = 0; j < numValues; j++) {
				data[i][j] = r.nextInt();
			}
			totalNumValues += numValues;
		}

		FixedByteWidthSingleColumnMultiValueWriter writer = new FixedByteWidthSingleColumnMultiValueWriter(
				file, rows, totalNumValues, 4);
		for (int i = 0; i < rows; i++) {
			writer.setIntArray(i, data[i]);
		}
		writer.close();
		DataInputStream dis = new DataInputStream(new FileInputStream(file));
		int cumLength = 0;
		for (int i = 0; i < rows; i++) {
			Assert.assertEquals(dis.readInt(), cumLength);
			Assert.assertEquals(dis.readInt(), data[i].length);
			cumLength += data[i].length;
		}
		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < data[i].length; j++) {
				Assert.assertEquals(dis.readInt(), data[i][j]);
			}
		}
		dis.close();
		file.delete();
	}
}
