package com.linkedin.pinot.index.reader;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Random;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.index.reader.impl.FixedBitWidthRowColDataFileReader;

@Test
public class TestFixedBitWidthRowColDataFileReader {
	private static Logger LOG = Logger
			.getLogger(TestFixedBitWidthRowColDataFileReader.class);
	boolean debug = false;

	@Test
	public void testReadIntFromByteBuffer() {
		int[] maxBitArray = new int[] { 5, 8, 10, 14, 16, 21, 24, 27, 31 };
		for (int maxBits : maxBitArray) {
			System.out.println("START MAX BITS:" + maxBits);
			int numElements = 100;
			BitSet bitset = new BitSet(numElements * maxBits);
			int max = (int) Math.pow(2, maxBits);
			Random r = new Random();
			int[] values = new int[numElements];
			for (int i = 0; i < numElements; i++) {
				int value = r.nextInt(max);
				values[i] = value;
				for (int j = 0; j < maxBits; j++) {
					if (((value >> j) & 1) == 1) {
						bitset.set(i * maxBits + j);
					}
				}
			}
			byte[] byteArray = bitset.toByteArray();
			ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
			int startBitPos = 0;
			int startByteOffset = 0;
			int endBitPos = 0;
			int endByteOffset = 0;
			for (int i = 0; i < numElements; i++) {
				startByteOffset = (maxBits * i) / 8;
				startBitPos = (maxBits * i) % 8;
				endByteOffset = (startByteOffset) + (maxBits + startBitPos - 1)
						/ 8;
				endBitPos = (maxBits * (i + 1) - 1) % 8;
				// for debugging
				if (debug) {
					System.out.println("row:" + i + "\n\tstartByteOffset:"
							+ startByteOffset + "\n\tstartBitPos:"
							+ startBitPos + "\n\tendByteOffset:"
							+ endByteOffset + "\n\tendBitPos:" + endBitPos);
				}
				int readInt = FixedBitWidthRowColDataFileReader.readInt(
						byteBuffer, startByteOffset, startBitPos,
						endByteOffset, endBitPos);
				System.out.println(i + "  Expected:" + values[i] + " Actual:"
						+ readInt);
				Assert.assertEquals(readInt, values[i]);
			}
			System.out.println("END MAX BITS:" + maxBits);

		}
	}

	public void testSingleCol() throws Exception {
		int[] maxBitArray = new int[] { 5, 8, 10, 14, 16, 21, 24, 27, 31 };

		for (int maxBits : maxBitArray) {
			String fileName = "test" + maxBits + "FixedBitWidthSingleCol";
			File file = new File(fileName);
			try {
				System.out.println("START MAX BITS:" + maxBits);
				int numElements = 100;
				BitSet bitset = new BitSet(numElements * maxBits);
				int max = (int) Math.pow(2, maxBits);
				Random r = new Random();
				int[] values = new int[numElements];
				for (int i = 0; i < numElements; i++) {
					int value = r.nextInt(max);
					values[i] = value;
					for (int j = 0; j < maxBits; j++) {
						if (((value >> j) & 1) == 1) {
							bitset.set(i * maxBits + j);
						}
					}
				}
				byte[] byteArray = bitset.toByteArray();

				FileOutputStream fos = new FileOutputStream(file);
				fos.write(byteArray);
				fos.close();
				FixedBitWidthRowColDataFileReader reader;
				reader = new FixedBitWidthRowColDataFileReader(fileName,
						numElements, 1, new int[] { maxBits });
				for (int i = 0; i < numElements; i++) {
					int readInt = reader.getInt(i, 0);
					System.out.println(i + "  Expected:" + values[i]
							+ " Actual:" + readInt);
					Assert.assertEquals(readInt, values[i]);
				}
				System.out.println("END MAX BITS:" + maxBits);
			} catch (Exception e) {
				LOG.error(e);
			} finally {
				file.delete();

			}

		}
	}
}
