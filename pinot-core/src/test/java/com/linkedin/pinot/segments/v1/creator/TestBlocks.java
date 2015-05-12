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
package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.index.reader.impl.FixedBitSkipListSCMVReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.data.source.mv.block.MultiValueBlock;
import com.linkedin.pinot.core.segment.index.data.source.mv.block.MultiValueBlockWithoutInvertedIndex;
import com.linkedin.pinot.core.segment.index.data.source.sv.block.SingleValueBlockWithoutInvertedIndex;
import com.linkedin.pinot.core.segment.index.data.source.sv.block.SortedSingleValueBlock;
import com.linkedin.pinot.core.segment.index.data.source.sv.block.UnSortedSingleValueBlock;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedMVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedSVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.SortedForwardIndexReader;
import com.linkedin.pinot.util.TestUtils;


public class TestBlocks {

  private static final String AVRO_DATA = "data/mirror-mv.avro";
  private static File INDEX_DIR = new File(FileUtils.getTempDirectory() + File.separator
      + TestIntArrays.class.getName());

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public static void before() throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(TestBlocks.class.getClassLoader().getResource(AVRO_DATA));
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    System.out.println(INDEX_DIR.getAbsolutePath());
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            TimeUnit.DAYS, "test", "testTable");
    config.setTimeColumnName("daysSinceEpoch");
    driver.init(config);
    driver.build();

    final DataFileStream<GenericRecord> avroReader = AvroUtils.getAvroReader(new File(filePath));
    final org.apache.avro.Schema avroSchema = avroReader.getSchema();
    final String[] columns = new String[avroSchema.getFields().size()];
    int i = 0;
    for (final Field f : avroSchema.getFields()) {
      columns[i] = f.name();
      i++;
    }
  }

  @Test(enabled = false)
  public void testSingleValueFilteredDocIdScanWithFiltering() throws Exception {
    File segmentDir = INDEX_DIR.listFiles()[0];
    IndexSegment segment = ColumnarSegmentLoader.load(segmentDir, ReadMode.mmap);
    SegmentMetadataImpl metadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    Schema schema = segment.getSegmentMetadata().getSchema();
    for (FieldSpec spec : schema.getAllFieldSpecs()) {
      if (!spec.isSingleValueField()) {
        continue;
      }

      System.out.println(" column : " + spec.getName());

      ColumnMetadata columnMetadata = metadata.getColumnMetadataFor(spec.getName());
      ColumnIndexContainer container =
          ColumnIndexContainer.init(metadata.getTableName(), spec.getName(), INDEX_DIR, columnMetadata, null,
              ReadMode.heap);

      SingleValueBlockWithoutInvertedIndex fwdIdxBlock =
          new SingleValueBlockWithoutInvertedIndex(new BlockId(0),
              (FixedBitCompressedSVForwardIndexReader) container.getForwardIndex(), container.getDictionary(),
              columnMetadata);
      ImmutableDictionaryReader dic = container.getDictionary();
      Object e = dic.get(new Random().nextInt(dic.length()));
      Predicate p = new EqPredicate(spec.getName(), Lists.newArrayList(e.toString()));

      Block invertedIndexBlock;
      if (columnMetadata.isSorted()) {
        invertedIndexBlock =
            new SortedSingleValueBlock(new BlockId(0), (SortedForwardIndexReader) container.getForwardIndex(), dic,
                columnMetadata);
      } else {
        invertedIndexBlock =
            new UnSortedSingleValueBlock(new BlockId(0),
                (FixedBitCompressedSVForwardIndexReader) container.getForwardIndex(), dic, columnMetadata);

        invertedIndexBlock.applyPredicate(p);
        fwdIdxBlock.applyPredicate(p);
      }
      BlockDocIdIterator it1 = invertedIndexBlock.getBlockDocIdSet().iterator();
      BlockDocIdIterator it2 = fwdIdxBlock.getBlockDocIdSet().iterator();

      int val1 = it1.next();
      int val2 = it2.next();
      while (val1 != Constants.EOF) {

        try {
          Assert.assertEquals(val1, val2);
        } catch (AssertionError e1) {
          StringBuilder b = new StringBuilder();
          for (int i = 0; i < dic.length(); i++) {
            b.append(dic.get(i) + ",");
          }
          System.out.println("all values : " + b.toString());
          System.out.println("looking for : " + e.toString() + " with dictionary id : " + dic.indexOf(e));
          System.out.println("val1:" + val1);
          System.out.println("val2:" + val2);
          System.out.println("column:" + spec.getName());
          System.out.println("sorted:" + columnMetadata.isSorted());
          throw new AssertionError(e1);
        }

        val1 = it1.next();
        val2 = it2.next();
      }
      try {
        Assert.assertEquals(val1, Constants.EOF);
        Assert.assertEquals(val2, Constants.EOF);
      } catch (AssertionError e1) {
        System.out.println("val1:" + val1);
        System.out.println("val2:" + val2);
        throw new AssertionError(e1);
      }

    }
  }

  @Test(enabled = false)
  public void testMultiValueFilteredDocIdScanWithFiltering() throws Exception {
    File segmentDir = INDEX_DIR.listFiles()[0];
    IndexSegment segment = ColumnarSegmentLoader.load(segmentDir, ReadMode.mmap);
    SegmentMetadataImpl metadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    Schema schema = segment.getSegmentMetadata().getSchema();
    for (FieldSpec spec : schema.getAllFieldSpecs()) {
      if (spec.isSingleValueField()) {
        continue;
      }

      ColumnMetadata columnMetadata = metadata.getColumnMetadataFor(spec.getName());
      ColumnIndexContainer container =
          ColumnIndexContainer.init(metadata.getTableName(), spec.getName(), INDEX_DIR, columnMetadata, null,
              ReadMode.heap);
      Dictionary dic = container.getDictionary();
      DataFileReader indexReader = container.getForwardIndex();
      InvertedIndexReader reader = container.getInvertedIndex();

      MultiValueBlockWithoutInvertedIndex fwdIdxBlock =
          new MultiValueBlockWithoutInvertedIndex(new BlockId(0), (FixedBitCompressedMVForwardIndexReader) indexReader,
              (ImmutableDictionaryReader) dic, columnMetadata);

      Object e = dic.get(new Random().nextInt(dic.length()));
      Predicate p = new EqPredicate(spec.getName(), Lists.newArrayList(e.toString()));

      Block invertedIndexBlock =
          new MultiValueBlock(new BlockId(0), (FixedBitSkipListSCMVReader) indexReader,
              (ImmutableDictionaryReader) dic, columnMetadata);

      invertedIndexBlock.applyPredicate(p);
      fwdIdxBlock.applyPredicate(p);

      BlockDocIdIterator it1 = invertedIndexBlock.getBlockDocIdSet().iterator();
      BlockDocIdIterator it2 = fwdIdxBlock.getBlockDocIdSet().iterator();

      int val1 = it1.next();
      int val2 = it2.next();
      while (val1 != Constants.EOF) {
        Assert.assertEquals(val1, val2);
        val1 = it1.next();
        val2 = it2.next();
      }

      Assert.assertEquals(val1, Constants.EOF);
      Assert.assertEquals(val2, Constants.EOF);

    }
  }
}
