package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnMetadata;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreator;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreatorFactory;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.indexsegment.utils.HeapCompressedIntArray;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;
import com.linkedin.pinot.core.indexsegment.utils.OffHeapCompressedIntArray;
import com.linkedin.pinot.core.indexsegment.utils.SortedIntArray;
import com.linkedin.pinot.core.time.SegmentTimeUnit;


public class TestIntArrays {
  private static final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File(TestIntArrays.class.toString());

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

    DataFileStream<GenericRecord> avroReader = AvroUtils.getAvroReader(new File(filePath));
    org.apache.avro.Schema avroSchema = avroReader.getSchema();
    String[] columns = new String[avroSchema.getFields().size()];
    int i = 0;
    for (Field f : avroSchema.getFields()) {
      columns[i] = f.name();
      i++;
    }
  }

  @Test
  public void test1() throws ConfigurationException, IOException {
    ColumnarSegment heapSegment = (ColumnarSegment) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
    ColumnarSegment mmapSegment = (ColumnarSegment) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.mmap);
    Map<String, ColumnMetadata> metadataMap = heapSegment.getColumnMetadataMap();

    for (String column : metadataMap.keySet()) {
      IntArray heapArray = heapSegment.getIntArrayFor(column);
      IntArray mmapArray = mmapSegment.getIntArrayFor(column);

      if (metadataMap.get(column).isSorted()) {
        AssertJUnit.assertEquals(heapArray instanceof SortedIntArray, true);
        AssertJUnit.assertEquals(mmapArray instanceof SortedIntArray, true);
      } else {
        AssertJUnit.assertEquals(heapArray instanceof HeapCompressedIntArray, true);
        AssertJUnit.assertEquals(mmapArray instanceof OffHeapCompressedIntArray, true);
      }

      for (int i = 0; i < metadataMap.get(column).getTotalDocs(); i++) {
        AssertJUnit.assertEquals(heapArray.getInt(i), mmapArray.getInt(i));
      }
    }
  }
}
