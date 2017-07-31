/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.chunk.creator.impl;

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
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.metadata.segment.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Nov 7, 2014
 */

public class ChunkIndexCreationDriverImplTest {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ChunkIndexCreationDriverImplTest.class);

  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static File INDEX_DIR = new File(ChunkIndexCreationDriverImplTest.class.toString());

  @BeforeClass
  public void setUP() throws Exception {

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final String filePath =
        TestUtils
            .getFileFromResourceUrl(ChunkIndexCreationDriverImplTest.class.getClassLoader().getResource(AVRO_DATA));

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            TimeUnit.DAYS, "testTable");
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void test2() throws Exception {
    final IndexSegmentImpl segment =
        (IndexSegmentImpl) Loaders.IndexSegment.load(INDEX_DIR.listFiles()[0], ReadMode.mmap);
//    System.out.println("INdex dir:" + INDEX_DIR);
    final DataSource ds = segment.getDataSource("column1");
    final Block bl = ds.nextBlock();
    final BlockValSet valSet = bl.getBlockValueSet();
    final BlockSingleValIterator it = (BlockSingleValIterator) valSet.iterator();
    // TODO: FIXME - load segment with known data and verify that it exists
    while (it.hasNext()) {
      LOGGER.trace(Integer.toString(it.nextIntVal()));
    }
  }

  @Test
  public void test3() throws Exception {
    final IndexSegmentImpl segment =
        (IndexSegmentImpl) Loaders.IndexSegment.load(INDEX_DIR.listFiles()[0], ReadMode.mmap);

    final DataSource ds = segment.getDataSource("column7");
    final Block bl = ds.nextBlock();
    final BlockValSet valSet = bl.getBlockValueSet();
    final int maxValue =
        ((SegmentMetadataImpl) segment.getSegmentMetadata()).getColumnMetadataFor("column7")
            .getMaxNumberOfMultiValues();

    final BlockMultiValIterator it = (BlockMultiValIterator) valSet.iterator();
    while (it.hasNext()) {
      final int[] entry = new int[maxValue];
      it.nextIntVal(entry);
      LOGGER.trace(Arrays.toString(entry));
    }
  }

  @Test(enabled = false)
  public void test4() throws Exception {
    final IndexSegmentImpl segment =
        (IndexSegmentImpl) Loaders.IndexSegment.load(INDEX_DIR.listFiles()[0], ReadMode.mmap);
    final ImmutableDictionaryReader d = segment.getDictionaryFor("column1");

    final List<String> rhs = new ArrayList<String>();
    rhs.add(d.get(new Random().nextInt(d.length())).toString());
    final Predicate p = new EqPredicate("column1", rhs);

    final DataSource ds = segment.getDataSource("column1", p);

    final Block bl = ds.nextBlock();
    final BlockDocIdSet idSet = bl.getBlockDocIdSet();

    final BlockDocIdIterator it = idSet.iterator();

    int docId = it.next();
    final StringBuilder b = new StringBuilder();
    while (docId != Constants.EOF) {
      b.append(docId + ",");
      docId = it.next();
    }
//    System.out.println(b.toString());
  }

  @Test(enabled = false)
  public void test5() throws Exception {
    final IndexSegmentImpl segment =
        (IndexSegmentImpl) Loaders.IndexSegment.load(INDEX_DIR.listFiles()[0], ReadMode.mmap);

    final List<String> rhs = new ArrayList<String>();
    rhs.add("-100");
    final Predicate p = new EqPredicate("column1", rhs);

    final DataSource ds = segment.getDataSource("column1", p);

    final Block bl = ds.nextBlock();
    final BlockDocIdSet idSet = bl.getBlockDocIdSet();

    final BlockDocIdIterator it = idSet.iterator();

    int docId = it.next();
    final StringBuilder b = new StringBuilder();
    while (docId != Constants.EOF) {
      b.append(docId + ",");
      docId = it.next();
    }
//    System.out.println(b.toString());
  }

  @Test(enabled = false)
  public void test6() throws Exception {
    final IndexSegmentImpl segment =
        (IndexSegmentImpl) Loaders.IndexSegment.load(INDEX_DIR.listFiles()[0], ReadMode.mmap);
    final ImmutableDictionaryReader d = segment.getDictionaryFor("column7");

    final List<String> rhs = new ArrayList<String>();
    rhs.add(d.get(new Random().nextInt(d.length())).toString());
    final Predicate p = new EqPredicate("column7", rhs);

    final DataSource ds = segment.getDataSource("column7", p);

    final Block bl = ds.nextBlock();
    final BlockDocIdSet idSet = bl.getBlockDocIdSet();

    final BlockDocIdIterator it = idSet.iterator();

    int docId = it.next();
    final StringBuilder b = new StringBuilder();
    while (docId != Constants.EOF) {
      b.append(docId + ",");
      docId = it.next();
    }
//    System.out.println(b.toString());
  }

}
