package com.linkedin.pinot.core.chunk.creator.impl;

import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.Predicate.Type;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 7, 2014
 */

public class TestChunkIndexCreationDriverImpl {

  private static final String AVRO_DATA = "data/mirror-mv.avro";
  private static File INDEX_DIR = new File(TestChunkIndexCreationDriverImpl.class.toString());

  @BeforeClass
  public void setUP() throws Exception {

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final String filePath = TestUtils
        .getFileFromResourceUrl(TestChunkIndexCreationDriverImpl.class.getClassLoader().getResource(AVRO_DATA));

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), new File(
            "/tmp/mirrorTwoDotO"), "daysSinceEpoch", TimeUnit.DAYS, "mirror", "mirror");
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
  }

  @Test
  public void test2() throws Exception {
    final IndexSegmentImpl segment =
        (IndexSegmentImpl) Loaders.IndexSegment.load(new File("/tmp/mirrorTwoDotO").listFiles()[0],
            ReadMode.mmap);

    final DataSource ds = segment.getDataSource("viewerPrivacySetting");
    final Block bl = ds.nextBlock();
    final BlockValSet valSet = bl.getBlockValueSet();
    final BlockSingleValIterator it = (BlockSingleValIterator) valSet.iterator();
    while (it.hasNext()) {
      System.out.println(it.nextIntVal());
    }
  }

  @Test
  public void test3() throws Exception {
    final IndexSegmentImpl segment =
        (IndexSegmentImpl) Loaders.IndexSegment.load(new File("/tmp/mirrorTwoDotO").listFiles()[0],
            ReadMode.mmap);

    final DataSource ds = segment.getDataSource("viewerCompanies");
    final Block bl = ds.nextBlock();
    final BlockValSet valSet = bl.getBlockValueSet();
    final int maxValue =
        ((SegmentMetadataImpl) segment.getSegmentMetadata()).getColumnMetadataFor("viewerCompanies")
            .getMaxNumberOfMultiValues();

    final BlockMultiValIterator it = (BlockMultiValIterator) valSet.iterator();
    while (it.hasNext()) {
      final int[] entry = new int[maxValue];
      it.nextIntVal(entry);
      System.out.println(Arrays.toString(entry));
    }
  }

  @Test
  public void test4() throws Exception {
    final IndexSegmentImpl segment =
        (IndexSegmentImpl) Loaders.IndexSegment.load(new File("/tmp/mirrorTwoDotO").listFiles()[0],
            ReadMode.mmap);
    final ImmutableDictionaryReader d = segment.getDictionaryFor("viewerId");

    final List<String> rhs = new ArrayList<String>();
    rhs.add(d.get(new Random().nextInt(d.length())).toString());
    final Predicate p = new Predicate("viewerId", Type.EQ, rhs);

    final DataSource ds = segment.getDataSource("viewerId", p);

    final Block bl = ds.nextBlock();
    final BlockDocIdSet idSet = bl.getBlockDocIdSet();

    final BlockDocIdIterator it = idSet.iterator();

    int docId = it.next();
    final StringBuilder b = new StringBuilder();
    while (docId != Constants.EOF) {
      b.append(docId + ",");
      docId = it.next();
    }
    System.out.println(b.toString());
  }

  @Test
  public void test5() throws Exception {
    final IndexSegmentImpl segment =
        (IndexSegmentImpl) Loaders.IndexSegment.load(new File("/tmp/mirrorTwoDotO").listFiles()[0],
            ReadMode.mmap);

    final List<String> rhs = new ArrayList<String>();
    rhs.add("-100");
    final Predicate p = new Predicate("viewerId", Type.EQ, rhs);

    final DataSource ds = segment.getDataSource("viewerId", p);

    final Block bl = ds.nextBlock();
    final BlockDocIdSet idSet = bl.getBlockDocIdSet();

    final BlockDocIdIterator it = idSet.iterator();

    int docId = it.next();
    final StringBuilder b = new StringBuilder();
    while (docId != Constants.EOF) {
      b.append(docId + ",");
      docId = it.next();
    }
    System.out.println(b.toString());
  }

  @Test
  public void test6() throws Exception {
    final IndexSegmentImpl segment =
        (IndexSegmentImpl) Loaders.IndexSegment.load(new File("/tmp/mirrorTwoDotO").listFiles()[0],
            ReadMode.mmap);
    final ImmutableDictionaryReader d = segment.getDictionaryFor("viewerOccupations");

    final List<String> rhs = new ArrayList<String>();
    rhs.add(d.get(new Random().nextInt(d.length())).toString());
    final Predicate p = new Predicate("viewerOccupations", Type.EQ, rhs);

    final DataSource ds = segment.getDataSource("viewerOccupations", p);

    final Block bl = ds.nextBlock();
    final BlockDocIdSet idSet = bl.getBlockDocIdSet();

    final BlockDocIdIterator it = idSet.iterator();

    int docId = it.next();
    final StringBuilder b = new StringBuilder();
    while (docId != Constants.EOF) {
      b.append(docId + ",");
      docId = it.next();
    }
    System.out.println(b.toString());
  }

}
