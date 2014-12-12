package com.linkedin.pinot.core.util;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.chunk.creator.impl.TestChunkIndexCreationDriverImpl;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.time.SegmentTimeUnit;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Dec 4, 2014
 */

public class TestCrcUtils {

  private static final String AVRO_DATA = "data/mirror-mv.avro";
  private static File INDEX_DIR = new File("/tmp/testingCrc");

  @Test
  public void test1() throws Exception {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final CrcUtils u1 = CrcUtils.forAllFilesInFolder(new File(makeSegmentAndReturnPath()));
    final long crc1 = u1.computeCrc();
    final String md51 = u1.computeMD5();

    FileUtils.deleteQuietly(INDEX_DIR);

    final CrcUtils u2 = CrcUtils.forAllFilesInFolder(new File(makeSegmentAndReturnPath()));
    final long crc2 = u2.computeCrc();
    final String md52 = u2.computeMD5();

    Assert.assertEquals(crc1, crc2);
    Assert.assertEquals(md51, md52);

    FileUtils.deleteQuietly(INDEX_DIR);

    final com.linkedin.pinot.core.indexsegment.IndexSegment segment = Loaders.IndexSegment.load(new File(makeSegmentAndReturnPath()), ReadMode.mmap);
    final SegmentMetadata m = segment.getSegmentMetadata();

    System.out.println(m.getCrc());
    System.out.println(m.getIndexCreationTime());

    FileUtils.deleteQuietly(INDEX_DIR);

  }

  private String makeSegmentAndReturnPath() throws Exception {
    final String filePath = TestChunkIndexCreationDriverImpl.class.getClassLoader().getResource(AVRO_DATA).getFile();

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR,
            "daysSinceEpoch", SegmentTimeUnit.days, "mirror", "mirror");
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    return new File(INDEX_DIR, driver.getSegmentName()).getAbsolutePath();
  }
}


//2894815653 2894815653
//388C7BA14F835B35D620949E7FF9FE46 388C7BA14F835B35D620949E7FF9FE46