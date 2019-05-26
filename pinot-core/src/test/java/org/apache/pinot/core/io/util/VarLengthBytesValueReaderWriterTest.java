package org.apache.pinot.core.io.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test for {@link VarLengthBytesValueReaderWriter}
 */
public class VarLengthBytesValueReaderWriterTest {
  private static final File TEMP_FILE = new File(FileUtils.getTempDirectory(),
      VarLengthBytesValueReaderWriterTest.class.getName());

  @Test
  public void testSingleByteArray() throws IOException {
    byte[] array = new byte[] {1, 2, 3, 4};
    byte[][] byteArrays = new byte[][]{array};
    long size = VarLengthBytesValueReaderWriter.getRequiredSize(byteArrays);
    Assert.assertEquals(16, size);

    try (PinotDataBuffer buffer = PinotDataBuffer
        .mapFile(TEMP_FILE, false, 0, size, ByteOrder.BIG_ENDIAN, null)) {
      VarLengthBytesValueReaderWriter readerWriter = new VarLengthBytesValueReaderWriter(buffer);
      readerWriter.init(byteArrays);
      Assert.assertEquals(1, readerWriter.getNumElements());
    }

    try (PinotDataBuffer buffer = PinotDataBuffer
        .mapFile(TEMP_FILE, true, 0, size, ByteOrder.BIG_ENDIAN, null)) {
      VarLengthBytesValueReaderWriter readerWriter = new VarLengthBytesValueReaderWriter(buffer);
      Assert.assertEquals(1, readerWriter.getNumElements());
      byte[] newArray = readerWriter.getBytes(0, -1, null);
      Assert.assertTrue(Arrays.equals(array, newArray));
    }
    finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  @Test
  public void testArbitraryLengthByteArray() throws IOException {
    Random random = new Random();
    int numByteArrays = random.nextInt(100);
    byte[][] byteArrays = new byte[numByteArrays][];
    for (int i = 0; i < numByteArrays; i++) {
      byteArrays[i] = new byte[i + 1];
      random.nextBytes(byteArrays[i]);
    }
    long size = VarLengthBytesValueReaderWriter.getRequiredSize(byteArrays);

    try (PinotDataBuffer buffer = PinotDataBuffer
        .mapFile(TEMP_FILE, false, 0, size, ByteOrder.BIG_ENDIAN, null)) {
      VarLengthBytesValueReaderWriter readerWriter = new VarLengthBytesValueReaderWriter(buffer);
      readerWriter.init(byteArrays);
      Assert.assertEquals(byteArrays.length, readerWriter.getNumElements());
    }

    try (PinotDataBuffer buffer = PinotDataBuffer
        .mapFile(TEMP_FILE, false, 0, size, ByteOrder.BIG_ENDIAN, null)) {
      VarLengthBytesValueReaderWriter readerWriter = new VarLengthBytesValueReaderWriter(buffer);
      Assert.assertEquals(byteArrays.length, readerWriter.getNumElements());
      for (int i = 0; i < byteArrays.length; i++) {
        byte[] array = byteArrays[i];
        byte[] newArray = readerWriter.getBytes(i, -1, null);
        Assert.assertTrue(Arrays.equals(array, newArray));
      }
    }
    finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }
}
