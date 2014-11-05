package com.linkedin.pinot.index.writer;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.BitSet;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.index.writer.impl.FixedBitWidthRowColDataFileWriter;

public class TestFixedBitWidthRowColDataFileWriter {

	@Test
	public void testSingleCol() throws Exception {
		int[] maxBitsArray = new int[] { 5, 10, 15, 25, 31 };
		for (int maxBits : maxBitsArray) {

			File file = new File("single_col_fixed_bit_" + maxBits + ".dat");
			int rows = 100;
			int cols = 1;
			int[] columnSizesInBits = new int[] { maxBits };
			FixedBitWidthRowColDataFileWriter writer = new FixedBitWidthRowColDataFileWriter(
					file, rows, cols, columnSizesInBits);
			int[] data = new int[rows];
			Random r = new Random();
			writer.open();
			int maxValue = (int) Math.pow(2, maxBits);
			BitSet set = new BitSet(rows * cols * maxBits);
			for (int i = 0; i < rows; i++) {
				data[i] = r.nextInt(maxValue);
				writer.setInt(i, 0, data[i]);
				for (int j = 0; j < maxBits; j++) {
					if (((data[i] >> j) & 1) == 1) {
						set.set(i * maxBits + j);
					}
				}
			}
			writer.saveAndClose();
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			byte[] b = new byte[(int) raf.length()];
			raf.read(b);
			byte[] byteArray = set.toByteArray();
			Assert.assertEquals(byteArray.length, b.length);
			Assert.assertEquals(byteArray, b);
			raf.close();
		}
	}
}
