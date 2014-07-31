package com.linkedin.pinot.segments.v1.creator;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.AssertJUnit;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.block.intarray.CompressedIntArrayBlock;
import com.linkedin.pinot.core.block.intarray.CompressedIntArrayDataSource;
import com.linkedin.pinot.core.block.intarray.CompressedIntBlockDocIdSet;
import com.linkedin.pinot.core.block.intarray.CompressedIntBlockDocIdValueSet;
import com.linkedin.pinot.core.block.intarray.CompressedIntBlockValSet;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.Predicate.Type;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnMetadata;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegment;
import com.linkedin.pinot.core.indexsegment.columnar.SegmentLoader;
import com.linkedin.pinot.core.indexsegment.columnar.SegmentLoader.IO_MODE;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreator;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreatorFactory;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.time.SegmentTimeUnit;


public class TestDataSourcesAndBlocks {
  private static final String AVRO_DATA = "data/sample_pv_data.avro";
  private static File INDEX_DIR = new File(TestDataSourcesAndBlocks.class.toString());

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public static void before() throws Exception {
    String filePath = TestDictionaries.class.getClassLoader().getResource(AVRO_DATA).getFile();
    if (INDEX_DIR.exists())
      FileUtils.deleteQuietly(INDEX_DIR);

    SegmentGeneratorConfiguration config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            SegmentTimeUnit.days, "test", "testTable");
    SegmentCreator cr = SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
    cr.init(config);
    cr.buildSegment();
  }

  @Test
  public void testBlockDocIdSingleColumnSetWithoutPredicate() throws IOException,
      org.apache.commons.configuration.ConfigurationException {
    ColumnarSegment heapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.heap);
    ColumnarSegment mmapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.mmap);
    Map<String, ColumnMetadata> metadataMap = heapSegment.getColumnMetadataMap();

    for (String column : metadataMap.keySet()) {
      CompressedIntArrayDataSource heapDatasource = (CompressedIntArrayDataSource) heapSegment.getDataSource(column);
      CompressedIntArrayDataSource mmapDataSource = (CompressedIntArrayDataSource) mmapSegment.getDataSource(column);

      CompressedIntArrayBlock heapBlock = (CompressedIntArrayBlock) heapDatasource.nextBlock();
      CompressedIntArrayBlock mmapBlock = (CompressedIntArrayBlock) mmapDataSource.nextBlock();

      CompressedIntBlockDocIdSet heapDocIdSet = (CompressedIntBlockDocIdSet) heapBlock.getBlockDocIdSet();
      CompressedIntBlockDocIdSet mmapDocIdSet = (CompressedIntBlockDocIdSet) mmapBlock.getBlockDocIdSet();

      BlockDocIdIterator heapIterator = heapDocIdSet.iterator();
      BlockDocIdIterator mmapIterator = mmapDocIdSet.iterator();

      int v1 = heapIterator.next();
      int v2 = mmapIterator.next();

      AssertJUnit.assertEquals(v1, v2);

      while (v1 != Constants.EOF) {
        AssertJUnit.assertEquals(v1, v2);
        v1 = heapIterator.next();
        v2 = mmapIterator.next();
      }
    }
  }

  @Test
  public void testBlockDocIdSingleColumnSetWithEqualsPredicate() throws IOException,
      org.apache.commons.configuration.ConfigurationException {
    ColumnarSegment heapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.heap);
    ColumnarSegment mmapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.mmap);
    Map<String, ColumnMetadata> metadataMap = heapSegment.getColumnMetadataMap();

    for (String column : metadataMap.keySet()) {
      Random r = new Random();
      Object randomValue =
          heapSegment.getDictionaryFor(column).getRaw(r.nextInt(heapSegment.getDictionaryFor(column).size()));

      List<String> rhs = new ArrayList<String>();
      rhs.add(randomValue.toString());
      Predicate p = new Predicate(column, Type.EQ, rhs);
      CompressedIntArrayDataSource heapDatasource = (CompressedIntArrayDataSource) heapSegment.getDataSource(column, p);
      heapDatasource.setPredicate(p);
      CompressedIntArrayDataSource mmapDataSource = (CompressedIntArrayDataSource) mmapSegment.getDataSource(column, p);
      mmapDataSource.setPredicate(p);

      CompressedIntArrayBlock heapBlock = (CompressedIntArrayBlock) heapDatasource.nextBlock();
      CompressedIntArrayBlock mmapBlock = (CompressedIntArrayBlock) mmapDataSource.nextBlock();

      CompressedIntBlockDocIdSet heapDocIdSet = (CompressedIntBlockDocIdSet) heapBlock.getBlockDocIdSet();
      CompressedIntBlockDocIdSet mmapDocIdSet = (CompressedIntBlockDocIdSet) mmapBlock.getBlockDocIdSet();

      BlockDocIdIterator heapIterator = heapDocIdSet.iterator();
      BlockDocIdIterator mmapIterator = mmapDocIdSet.iterator();

      int counter = 0;

      int v1 = heapIterator.next();
      int v2 = mmapIterator.next();
      AssertJUnit.assertEquals(v1, v2);

      counter++;

      while (v1 != Constants.EOF || v2 != Constants.EOF) {
        AssertJUnit.assertEquals(v1, v2);
        v1 = heapIterator.next();
        v2 = mmapIterator.next();
        counter++;
      }

      //Assert.assertEquals(counter <= metadataMap.get(column).getTotalDocs(), true);
    }
  }
  
  @Test
  public void testBlockDocIdSingleColumnSetWithNotEqualsPredicate() throws IOException,
      org.apache.commons.configuration.ConfigurationException {
    ColumnarSegment heapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.heap);
    ColumnarSegment mmapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.mmap);
    Map<String, ColumnMetadata> metadataMap = heapSegment.getColumnMetadataMap();

    for (String column : metadataMap.keySet()) {
      Random r = new Random();
      Object randomValue =
          heapSegment.getDictionaryFor(column).getRaw(r.nextInt(heapSegment.getDictionaryFor(column).size()));

      List<String> rhs = new ArrayList<String>();
      rhs.add(randomValue.toString());
      Predicate p = new Predicate(column, Type.NEQ, rhs);
      CompressedIntArrayDataSource heapDatasource = (CompressedIntArrayDataSource) heapSegment.getDataSource(column, p);
      heapDatasource.setPredicate(p);
      CompressedIntArrayDataSource mmapDataSource = (CompressedIntArrayDataSource) mmapSegment.getDataSource(column, p);
      mmapDataSource.setPredicate(p);

      CompressedIntArrayBlock heapBlock = (CompressedIntArrayBlock) heapDatasource.nextBlock();
      CompressedIntArrayBlock mmapBlock = (CompressedIntArrayBlock) mmapDataSource.nextBlock();

      CompressedIntBlockDocIdSet heapDocIdSet = (CompressedIntBlockDocIdSet) heapBlock.getBlockDocIdSet();
      CompressedIntBlockDocIdSet mmapDocIdSet = (CompressedIntBlockDocIdSet) mmapBlock.getBlockDocIdSet();

      BlockDocIdIterator heapIterator = heapDocIdSet.iterator();
      BlockDocIdIterator mmapIterator = mmapDocIdSet.iterator();

      int counter = 0;

      int v1 = heapIterator.next();
      int v2 = mmapIterator.next();
      AssertJUnit.assertEquals(v1, v2);

      counter++;

      while (v1 != Constants.EOF || v2 != Constants.EOF) {
        AssertJUnit.assertEquals(v1, v2);
        v1 = heapIterator.next();
        v2 = mmapIterator.next();
        counter++;
      }

      //Assert.assertEquals(counter <= metadataMap.get(column).getTotalDocs(), true);
    }
  }
  
  @Test
  public void testBlockDocIdValSetI() throws IOException, org.apache.commons.configuration.ConfigurationException {
    ColumnarSegment heapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.heap);
    ColumnarSegment mmapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.mmap);
    Map<String, ColumnMetadata> metadataMap = heapSegment.getColumnMetadataMap();

    for (String column : metadataMap.keySet()) {
      CompressedIntArrayDataSource heapDatasource = (CompressedIntArrayDataSource) heapSegment.getDataSource(column);
      CompressedIntArrayDataSource mmapDataSource = (CompressedIntArrayDataSource) mmapSegment.getDataSource(column);

      CompressedIntArrayBlock heapBlock = (CompressedIntArrayBlock) heapDatasource.nextBlock();
      CompressedIntArrayBlock mmapBlock = (CompressedIntArrayBlock) mmapDataSource.nextBlock();

      CompressedIntBlockDocIdValueSet heapDocIdValueSet =
          (CompressedIntBlockDocIdValueSet) heapBlock.getBlockDocIdValueSet();
      CompressedIntBlockDocIdValueSet mmapDocIdValueSet =
          (CompressedIntBlockDocIdValueSet) mmapBlock.getBlockDocIdValueSet();
    }
  }

  @Test
  public void testBlockValSetI() throws IOException, org.apache.commons.configuration.ConfigurationException {
    ColumnarSegment heapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.heap);
    ColumnarSegment mmapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.mmap);
    Map<String, ColumnMetadata> metadataMap = heapSegment.getColumnMetadataMap();

    for (String column : metadataMap.keySet()) {
      CompressedIntArrayDataSource heapDatasource = (CompressedIntArrayDataSource) heapSegment.getDataSource(column);
      CompressedIntArrayDataSource mmapDataSource = (CompressedIntArrayDataSource) mmapSegment.getDataSource(column);

      CompressedIntArrayBlock heapBlock = (CompressedIntArrayBlock) heapDatasource.nextBlock();
      CompressedIntArrayBlock mmapBlock = (CompressedIntArrayBlock) mmapDataSource.nextBlock();

      CompressedIntBlockValSet heapValSet = (CompressedIntBlockValSet) heapBlock.getBlockValueSet();
      CompressedIntBlockValSet mmapValSet = (CompressedIntBlockValSet) mmapBlock.getBlockValueSet();
    }
  }

}
