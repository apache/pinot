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
package org.apache.pinot.segment.spi.memory;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;


/**
 * Not an actual test but a base class that can be extended by tests in order to have some basic data
 */
public class PinotDataBufferTestBase {

  protected static final Random RANDOM = new Random();
  protected ExecutorService _executorService;
  protected static final File TEMP_FILE = new File(FileUtils.getTempDirectory(), "PinotDataBufferTest");
  protected static final int FILE_OFFSET = 10;      // Not page-aligned
  protected static final int BUFFER_SIZE = 10_000;  // Not page-aligned
  protected static final int CHAR_ARRAY_LENGTH = BUFFER_SIZE / Character.BYTES;
  protected static final int SHORT_ARRAY_LENGTH = BUFFER_SIZE / Short.BYTES;
  protected static final int INT_ARRAY_LENGTH = BUFFER_SIZE / Integer.BYTES;
  protected static final int LONG_ARRAY_LENGTH = BUFFER_SIZE / Long.BYTES;
  protected static final int FLOAT_ARRAY_LENGTH = BUFFER_SIZE / Float.BYTES;
  protected static final int DOUBLE_ARRAY_LENGTH = BUFFER_SIZE / Double.BYTES;
  protected static final int NUM_ROUNDS = 1000;
  protected static final int MAX_BYTES_LENGTH = 100;
  protected static final long LARGE_BUFFER_SIZE = Integer.MAX_VALUE + 2L; // Not page-aligned

  protected byte[] _bytes = new byte[BUFFER_SIZE];
  protected char[] _chars = new char[CHAR_ARRAY_LENGTH];
  protected short[] _shorts = new short[SHORT_ARRAY_LENGTH];
  protected int[] _ints = new int[INT_ARRAY_LENGTH];
  protected long[] _longs = new long[LONG_ARRAY_LENGTH];
  protected float[] _floats = new float[FLOAT_ARRAY_LENGTH];
  protected double[] _doubles = new double[DOUBLE_ARRAY_LENGTH];

  @BeforeClass
  public void setUp() {
    for (int i = 0; i < BUFFER_SIZE; i++) {
      _bytes[i] = (byte) RANDOM.nextInt();
    }
    for (int i = 0; i < CHAR_ARRAY_LENGTH; i++) {
      _chars[i] = (char) RANDOM.nextInt();
    }
    for (int i = 0; i < SHORT_ARRAY_LENGTH; i++) {
      _shorts[i] = (short) RANDOM.nextInt();
    }
    for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
      _ints[i] = RANDOM.nextInt();
    }
    for (int i = 0; i < LONG_ARRAY_LENGTH; i++) {
      _longs[i] = RANDOM.nextLong();
    }
    for (int i = 0; i < FLOAT_ARRAY_LENGTH; i++) {
      _floats[i] = RANDOM.nextFloat();
    }
    for (int i = 0; i < DOUBLE_ARRAY_LENGTH; i++) {
      _doubles[i] = RANDOM.nextDouble();
    }
  }

  protected void testBufferStats(int directBufferCount, long directBufferUsage, int mmapBufferCount,
      long mmapBufferUsage) {
    Assert.assertEquals(PinotDataBuffer.getAllocationFailureCount(), 0);
    Assert.assertEquals(PinotDataBuffer.getDirectBufferCount(), directBufferCount);
    Assert.assertEquals(PinotDataBuffer.getDirectBufferUsage(), directBufferUsage);
    Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), mmapBufferCount);
    Assert.assertEquals(PinotDataBuffer.getMmapBufferUsage(), mmapBufferUsage);
    Assert.assertEquals(PinotDataBuffer.getBufferInfo().size(), directBufferCount + mmapBufferCount);
  }

  @AfterMethod
  public void deleteFileIfExists()
      throws IOException {
    if (TEMP_FILE.exists()) {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  @BeforeMethod
  public void setupExecutor() {
    _executorService = Executors.newFixedThreadPool(10);
  }

  @AfterMethod
  public void tearDown() {
    if (_executorService != null) {
      _executorService.shutdown();
    }
  }
}
