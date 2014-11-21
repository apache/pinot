package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.chunk.creator.ChunkIndexCreationDriver;
import com.linkedin.pinot.core.chunk.index.ChunkColumnMetadata;
import com.linkedin.pinot.core.chunk.index.ColumnarChunk;
import com.linkedin.pinot.core.chunk.index.ColumnarChunkMetadata;
import com.linkedin.pinot.core.chunk.index.readers.FixedBitCompressedMVForwardIndexReader;
import com.linkedin.pinot.core.chunk.index.readers.FixedBitCompressedSVForwardIndexReader;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.time.SegmentTimeUnit;


public class TestIntArrays {
  private static final String AVRO_DATA = "data/mirror-mv.avro";
  private static File INDEX_DIR = new File(TestIntArrays.class.toString());

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public static void before() throws Exception {
    final String filePath = TestDictionaries.class.getClassLoader().getResource(AVRO_DATA).getFile();
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final ChunkIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            SegmentTimeUnit.days, "test", "testTable");
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

  @Test
  public void test1() throws Exception {
    final ColumnarChunk heapSegment = (ColumnarChunk) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
    final ColumnarChunk mmapSegment = (ColumnarChunk) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.mmap);
    final Map<String, ChunkColumnMetadata> metadataMap = ((ColumnarChunkMetadata)heapSegment.getSegmentMetadata()).getColumnMetadataMap();

    for (final String column : metadataMap.keySet()) {

      final DataFileReader heapArray = heapSegment.getForwardIndexReaderFor(column);
      final DataFileReader mmapArray = mmapSegment.getForwardIndexReaderFor(column);

      if (metadataMap.get(column).isSingleValue()) {
        final FixedBitCompressedSVForwardIndexReader svHeapReader = (FixedBitCompressedSVForwardIndexReader) heapArray;
        final FixedBitCompressedSVForwardIndexReader mvMmapReader = (FixedBitCompressedSVForwardIndexReader) mmapArray;
        for (int i = 0; i < metadataMap.get(column).getTotalDocs(); i++) {
          AssertJUnit.assertEquals(svHeapReader.getInt(i), mvMmapReader.getInt(i));
        }
      } else {
        final FixedBitCompressedMVForwardIndexReader svHeapReader = (FixedBitCompressedMVForwardIndexReader) heapArray;
        final FixedBitCompressedMVForwardIndexReader mvMmapReader = (FixedBitCompressedMVForwardIndexReader) mmapArray;
        for (int i = 0; i < metadataMap.get(column).getTotalDocs(); i++) {
          final int[] i_1 = new int[1000];
          final int[] j_i = new int[1000];
          AssertJUnit.assertEquals(svHeapReader.getIntArray(i, i_1), mvMmapReader.getIntArray(i, j_i));
        }
      }
    }
  }
}
