package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.readers.text.NativeTextIndexReader;
import org.apache.pinot.segment.local.utils.nativefst.NativeTextIndexCreator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.NATIVE_TEXT_INDEX_FILE_EXTENSION;


public class NativeTextIndexCreatorTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NativeTextIndexCreatorTest");

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testIndexWriterReader()
      throws IOException {
    String[] uniqueValues = new String[4];
    uniqueValues[0] = "hello-world";
    uniqueValues[1] = "hello-world123";
    uniqueValues[2] = "still";
    uniqueValues[3] = "zoobar";

    try (NativeTextIndexCreator creator = new NativeTextIndexCreator("testFSTColumn", INDEX_DIR)) {
      creator.add(uniqueValues, 4);
      creator.seal();
    }

    File fstFile = new File(INDEX_DIR, "testFSTColumn" + NATIVE_TEXT_INDEX_FILE_EXTENSION);
    try (NativeTextIndexReader reader = new NativeTextIndexReader("testFSTColumn", fstFile.getParentFile(), 4)) {

      int[] matchedDictIds = reader.getDictIds("hello.*").toArray();
      Assert.assertEquals(1, matchedDictIds.length);
      Assert.assertEquals(0, matchedDictIds[0]);

      matchedDictIds = reader.getDictIds(".*llo").toArray();
      Assert.assertEquals(1, matchedDictIds.length);
      Assert.assertEquals(0, matchedDictIds[0]);

      matchedDictIds = reader.getDictIds("wor.*").toArray();
      Assert.assertEquals(2, matchedDictIds.length);
      Assert.assertEquals(2, matchedDictIds[0]);
      Assert.assertEquals(3, matchedDictIds[1]);

      int[] matchedDocIds = reader.getDocIds("hello.*").toArray();
      Assert.assertEquals(2, matchedDocIds.length);
      Assert.assertEquals(0, matchedDocIds[0]);
      Assert.assertEquals(1, matchedDocIds[1]);

      matchedDocIds = reader.getDocIds(".*llo").toArray();
      Assert.assertEquals(2, matchedDocIds.length);
      Assert.assertEquals(0, matchedDocIds[0]);
      Assert.assertEquals(1, matchedDocIds[1]);

      matchedDocIds = reader.getDocIds("wor.*").toArray();
      Assert.assertEquals(2, matchedDocIds.length);
      Assert.assertEquals(0, matchedDocIds[0]);
      Assert.assertEquals(1, matchedDocIds[1]);

      matchedDocIds = reader.getDocIds("zoo.*").toArray();
      Assert.assertEquals(1, matchedDocIds.length);
      Assert.assertEquals(3, matchedDocIds[0]);
    }
  }
}
