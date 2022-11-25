package org.apache.pinot.segment.spi.memory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;


public class PinotDataBufferTestBase {

  protected static final Random RANDOM = new Random();
  protected static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(10);
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

  @AfterTest
  public void deleteFileIfExists()
      throws IOException {
    if (TEMP_FILE.exists()) {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  @AfterClass
  public void tearDown() {
    EXECUTOR_SERVICE.shutdown();
  }
}
