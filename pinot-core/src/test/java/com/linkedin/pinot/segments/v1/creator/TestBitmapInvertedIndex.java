package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.io.FileInputStream;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.chunk.creator.ChunkIndexCreationDriver;
import com.linkedin.pinot.core.chunk.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.chunk.index.ColumnarChunk;
import com.linkedin.pinot.core.chunk.index.ColumnarChunkMetadata;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.time.SegmentTimeUnit;


public class TestBitmapInvertedIndex {
  private final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File(TestBitmapInvertedIndex.class.toString());

  @Test
  public void test1() throws Exception {
    // load segment in heap mode
    final ColumnarChunk heapSegment = (ColumnarChunk) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
    // compare the loaded inverted index with the record in avro file
    final DataFileStream<GenericRecord> reader =
        new DataFileStream<GenericRecord>(new FileInputStream(new File(getClass().getClassLoader()
            .getResource(AVRO_DATA).getFile())), new GenericDatumReader<GenericRecord>());
    int docId = 0;
    while (reader.hasNext()) {
      final GenericRecord rec = reader.next();
      for (final String column : ((ColumnarChunkMetadata)heapSegment.getSegmentMetadata()).getColumnMetadataMap().keySet()) {
        Object entry = rec.get(column);
        if (entry instanceof Utf8) {
          entry = ((Utf8) entry).toString();
        }
        final int dicId = heapSegment.getDictionaryFor(column).indexOf(entry);
        // make sure that docId for dicId exist in the inverted index
        AssertJUnit.assertEquals(heapSegment.getInvertedIndexFor(column).getImmutable(dicId).contains(docId), true);
        final int size = heapSegment.getDictionaryFor(column).length();
        for (int i = 0; i < size; ++i) { // remove this for-loop for quick test
          if (i == dicId) {
            continue;
          }
          // make sure that docId for dicId does not exist in the inverted index
          AssertJUnit.assertEquals(heapSegment.getInvertedIndexFor(column).getImmutable(i).contains(docId), false);
        }
      }
      ++docId;
    }
  }

  @Test
  public void test2() throws Exception {
    // load segment in mmap mode
    final ColumnarChunk mmapSegment = (ColumnarChunk) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.mmap);
    // compare the loaded inverted index with the record in avro file
    final DataFileStream<GenericRecord> reader =
        new DataFileStream<GenericRecord>(new FileInputStream(new File(getClass().getClassLoader()
            .getResource(AVRO_DATA).getFile())), new GenericDatumReader<GenericRecord>());
    int docId = 0;
    while (reader.hasNext()) {
      final GenericRecord rec = reader.next();
      for (final String column : ((ColumnarChunkMetadata)mmapSegment.getSegmentMetadata()).getColumnMetadataMap().keySet()) {
        Object entry = rec.get(column);
        if (entry instanceof Utf8) {
          entry = ((Utf8) entry).toString();
        }
        final int dicId = mmapSegment.getDictionaryFor(column).indexOf(entry);
        // make sure that docId for dicId exist in the inverted index
        AssertJUnit.assertEquals(mmapSegment.getInvertedIndexFor(column).getImmutable(dicId).contains(docId), true);
        final int size = mmapSegment.getDictionaryFor(column).length();
        for (int i = 0; i < size; ++i) { // remove this for-loop for quick test
          if (i == dicId) {
            continue;
          }
          // make sure that docId for dicId does not exist in the inverted index
          AssertJUnit.assertEquals(mmapSegment.getInvertedIndexFor(column).getImmutable(i).contains(docId), false);
        }
      }
      ++docId;
    }
  }

  @AfterClass
  public void teardown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public void setup() throws Exception {
    final String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            SegmentTimeUnit.days, "test", "testTable");

    final ChunkIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }
}
