package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.forward.BaseChunkSVForwardIndexReader.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkMVForwardIndexReader;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MultiValueVarByteRawIndexCreatorTest {

  private static final String OUTPUT_DIR =
      System.getProperty("java.io.tmpdir") + File.separator + "mvVarRawTest";

  @BeforeClass
  public void setup() throws Exception {
    FileUtils.forceMkdir(new File(OUTPUT_DIR));
  }

  /**
   * Clean up after test
   */
  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(new File(OUTPUT_DIR));
  }

  @Test
  public void testMV() throws IOException {
    String column = "testCol";
    int numDocs = 1000;
    int maxElements = 50;
    int maxTotalLength = 500;
    File file = new File(OUTPUT_DIR, column + Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);
    file.delete();
    MultiValueVarByteRawIndexCreator creator = new MultiValueVarByteRawIndexCreator(
        new File(OUTPUT_DIR), ChunkCompressionType.SNAPPY, column, numDocs, DataType.STRING,
        maxTotalLength, maxElements);
    List<String[]> inputs = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < numDocs; i++) {
      //int length = 1;
      int length = random.nextInt(10);
      String[] values = new String[length];
      for (int j = 0; j < length; j++) {
        char[] value = new char[length];
        Arrays.fill(value, 'a');
        values[j] = new String(value);
      }
      inputs.add(values);
      creator.putStringMV(values);
    }
    creator.close();

    //read
    final PinotDataBuffer buffer = PinotDataBuffer
        .mapFile(file, true, 0, file.length(), ByteOrder.BIG_ENDIAN, "");
    VarByteChunkMVForwardIndexReader reader = new VarByteChunkMVForwardIndexReader(buffer,
        DataType.STRING);
    final ChunkReaderContext context = reader.createContext();
    String[] values = new String[maxElements];
    for (int i = 0; i < numDocs; i++) {
      int length = reader.getStringMV(i, values, context);
      String[] readValue = Arrays.copyOf(values, length);
      Assert.assertEquals(inputs.get(i), readValue);
    }
  }
}
