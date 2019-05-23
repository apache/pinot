package org.apache.pinot.core.io.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test for {@link VarLengthStringsReaderWriter}
 */
public class VarLengthStringsReaderWriterTest {
  private static final File TEMP_FILE = new File(FileUtils.getTempDirectory(),
      VarLengthBytesValueReaderWriterTest.class.getName());

  @Test
  public void testSingleString() throws IOException {
    String[] strings = new String[] {"random"};
    long size = VarLengthStringsReaderWriter.getRequiredSize(strings);
    Assert.assertEquals(14, size);

    try (PinotDataBuffer buffer = PinotDataBuffer
        .mapFile(TEMP_FILE, false, 0, size, ByteOrder.BIG_ENDIAN, null)) {
      VarLengthStringsReaderWriter readerWriter = new VarLengthStringsReaderWriter(buffer);
      readerWriter.init(strings);
      Assert.assertEquals(1, readerWriter.getNumElements());

      readerWriter = new VarLengthStringsReaderWriter(buffer);
      Assert.assertEquals(1, readerWriter.getNumElements());
      Assert.assertEquals("random", readerWriter.getString(0));
    }
    finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  @Test
  public void testArbitraryLengthStrings() throws IOException {
    Random random = new Random();
    int numStrings = random.nextInt(100);
    String[] strings = new String[numStrings];
    for (int i = 0; i < numStrings; i++) {
      // Generate strings of different length.
      byte[] array = new byte[random.nextInt(20)];
      new Random().nextBytes(array);
      strings[i] = new String(array, Charset.forName("UTF-8"));
    }
    long size = VarLengthStringsReaderWriter.getRequiredSize(strings);

    try (PinotDataBuffer buffer = PinotDataBuffer
        .mapFile(TEMP_FILE, false, 0, size, ByteOrder.BIG_ENDIAN, null)) {
      VarLengthStringsReaderWriter readerWriter = new VarLengthStringsReaderWriter(buffer);
      readerWriter.init(strings);
      Assert.assertEquals(strings.length, readerWriter.getNumElements());

      readerWriter = new VarLengthStringsReaderWriter(buffer);
      Assert.assertEquals(strings.length, readerWriter.getNumElements());
      for (int i = 0; i < strings.length; i++) {
        Assert.assertEquals(strings[i], readerWriter.getString(i));
      }
    }
    finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }
}

