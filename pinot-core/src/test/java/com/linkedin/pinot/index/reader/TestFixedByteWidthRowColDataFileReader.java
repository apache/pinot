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
package com.linkedin.pinot.index.reader;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.index.reader.impl.FixedByteWidthRowColDataFileReader;

@Test
public class TestFixedByteWidthRowColDataFileReader {

	@Test
	void testSingleCol() throws Exception {
		String fileName = "test_single_col.dat";
		File f = new File(fileName);
		f.delete();
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(f));
		int[] data = new int[100];
		Random r = new Random();
		for (int i = 0; i < data.length; i++) {
			data[i] = r.nextInt();
			dos.writeInt(data[i]);
		}
		dos.flush();
		dos.close();
		RandomAccessFile raf = new RandomAccessFile(f, "rw");
		System.out.println("file size: " + raf.getChannel().size());
		FixedByteWidthRowColDataFileReader reader = new FixedByteWidthRowColDataFileReader(
				fileName, data.length, 1, new int[] { 4 });
		reader.open();
		for (int i = 0; i < data.length; i++) {
			Assert.assertEquals(reader.getInt(i, 0), data[i]);
		}
		reader.close();
		raf.close();
		f.delete();
	}

	@Test
	void testMultiCol() throws Exception {
		String fileName = "test_single_col.dat";
		File f = new File(fileName);
		f.delete();
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(f));

		int numRows = 100;
		int numCols = 2;
		int[][] colData = new int[numRows][numCols];
		Random r = new Random();

		for (int i = 0; i < numRows; i++) {
			colData[i] = new int[numCols];
			for (int j = 0; j < numCols; j++) {
				colData[i][j] = r.nextInt();
				dos.writeInt(colData[i][j]);
			}
		}
		dos.flush();
		dos.close();
		RandomAccessFile raf = new RandomAccessFile(f, "rw");
		System.out.println("file size: " + raf.getChannel().size());
		FixedByteWidthRowColDataFileReader reader = new FixedByteWidthRowColDataFileReader(
				fileName, numRows, numCols, new int[] { 4, 4 });
		reader.open();
		for (int i = 0; i < numRows; i++) {
			for (int j = 0; j < numCols; j++) {
				Assert.assertEquals(reader.getInt(i, j), colData[i][j]);
			}
		}
		reader.close();
		raf.close();
		f.delete();
	}

}
