package com.linkedin.pinot.index.reader;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.BitSet;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.index.reader.impl.FixedBitWidthSingleColumnMultiValueReader;

public class TestFixedBitWidthSingleColumnMultiValueReader {

	@Test
	public void testSingleColMultiValue() throws Exception {
		String fileName = "test_single_col_mv_fixed_bit.dat";
		File f = new File(fileName);
		f.delete();
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(f));
		int[][] data = new int[100][];
		Random r = new Random();
		int maxBits = 5;
		int maxValue = (int) Math.pow(2, maxBits);
		// generate the
		for (int i = 0; i < data.length; i++) {
			int numValues = r.nextInt(100) + 1;
			data[i] = new int[numValues];
			for (int j = 0; j < numValues; j++) {
				data[i][j] = r.nextInt(maxValue);
			}
		}
		int cumValues = 0;
		// write the header section
		for (int i = 0; i < data.length; i++) {
			dos.writeInt(cumValues);
			dos.writeInt(data[i].length);
			cumValues += data[i].length;
		}
		// write the data section
		BitSet bitSet = new BitSet(cumValues * maxBits);
		int index = 0;
		for (int i = 0; i < data.length; i++) {
			int numValues = data[i].length;
			for (int j = 0; j < numValues; j++) {
				for (int k = 0; k < maxBits; k++) {
					if (((data[i][j] >> k) & 1) == 1) {
						bitSet.set((index* maxBits) + k);
					}
				}
				index = index + 1;
			}
		}
		dos.write(bitSet.toByteArray());
		dos.flush();
		dos.close();
		RandomAccessFile raf = new RandomAccessFile(f, "rw");
		System.out.println("file size: " + raf.getChannel().size());
		FixedBitWidthSingleColumnMultiValueReader reader;
		reader = new FixedBitWidthSingleColumnMultiValueReader(f, data.length,
				maxBits, true);
		reader.open();
		int[] readValues = new int[100];
		for (int i = 0; i < data.length; i++) {
			int numValues = reader.getIntArray(i, readValues);
			Assert.assertEquals(numValues, data[i].length);
			for (int j = 0; j < numValues; j++) {
				Assert.assertEquals(readValues[j], data[i][j]);
			}
		}
		reader.close();
		raf.close();
		f.delete();
	}
}
