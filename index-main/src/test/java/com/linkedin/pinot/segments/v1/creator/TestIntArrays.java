package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.index.data.Schema;
import com.linkedin.pinot.index.time.SegmentTimeUnit;
import com.linkedin.pinot.raw.record.readers.RecordReaderFactory;
import com.linkedin.pinot.segments.creator.SegmentCreator;
import com.linkedin.pinot.segments.creator.SegmentCreatorFactory;
import com.linkedin.pinot.segments.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.segments.generator.SegmentVersion;
import com.linkedin.pinot.segments.utils.AvroUtils;
import com.linkedin.pinot.segments.v1.segment.ColumnMetadata;
import com.linkedin.pinot.segments.v1.segment.ColumnarSegment;
import com.linkedin.pinot.segments.v1.segment.SegmentLoader;
import com.linkedin.pinot.segments.v1.segment.SegmentLoader.IO_MODE;
import com.linkedin.pinot.segments.v1.segment.utils.HeapCompressedIntArray;
import com.linkedin.pinot.segments.v1.segment.utils.IntArray;
import com.linkedin.pinot.segments.v1.segment.utils.OffHeapCompressedIntArray;
import com.linkedin.pinot.segments.v1.segment.utils.SortedIntArray;


public class TestIntArrays {
  private static final String AVRO_DATA = "data/sample_pv_data.avro";
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
    ColumnarSegment heapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.heap);
    ColumnarSegment mmapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.mmap);
    Map<String, ColumnMetadata> metadataMap = heapSegment.getColumnMetadataMap();

    for (String column : metadataMap.keySet()) {
      IntArray heapArray = heapSegment.getIntArrayFor(column);
      IntArray mmapArray = mmapSegment.getIntArrayFor(column);
      
      if (metadataMap.get(column).isSorted()) {
        Assert.assertEquals(heapArray instanceof SortedIntArray, true);
        Assert.assertEquals(mmapArray instanceof SortedIntArray, true);
      } else {
        Assert.assertEquals(heapArray instanceof HeapCompressedIntArray, true);
        Assert.assertEquals(mmapArray instanceof OffHeapCompressedIntArray, true);
      }

      for (int i = 0; i < metadataMap.get(column).getTotalDocs(); i++) {
        Assert.assertEquals(heapArray.getInt(i), mmapArray.getInt(i));
      }
    }
  }
}
