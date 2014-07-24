package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.index.data.FieldSpec.DataType;
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
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.heap.InMemoryDoubleDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.heap.InMemoryFloatDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.heap.InMemoryIntDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.heap.InMemoryLongDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.heap.InMemoryStringDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.mmap.MmapDoubleDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.mmap.MmapFloatDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.mmap.MmapIntDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.mmap.MmapLongDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.mmap.MmapStringDictionary;


public class TestDictionaryCreators {
  private static final String AVRO_DATA = "data/sample_pv_data.avro";
  private static File INDEX_DIR = new File(TestDictionaryCreators.class.toString());
  static Map<String, Set<Object>> uniqueEntries;

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public static void before() throws Exception {
    String filePath = TestDictionaryCreators.class.getClassLoader().getResource(AVRO_DATA).getFile();
    System.out.println(filePath);
    if (INDEX_DIR.exists())
      FileUtils.deleteQuietly(INDEX_DIR);

    SegmentGeneratorConfiguration config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            SegmentTimeUnit.days, "test", "testTable");
    SegmentCreator cr = SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
    cr.init(config);
    cr.buildSegment();
    Schema schema = AvroUtils.extractSchemaFromAvro(new File(filePath));

    DataFileStream<GenericRecord> avroReader = AvroUtils.getAvroReader(new File(filePath));
    org.apache.avro.Schema avroSchema = avroReader.getSchema();
    String[] columns = new String[avroSchema.getFields().size()];
    int i = 0;
    for (Field f : avroSchema.getFields()) {
      columns[i] = f.name();
      i++;
    }

    uniqueEntries = new HashMap<>();
    for (String column : columns) {
      uniqueEntries.put(column, new HashSet<>());
    }

    while (avroReader.hasNext()) {
      GenericRecord rec = avroReader.next();
      for (String column : columns) {
        Object val = rec.get(column);
        if (val instanceof Utf8) {
          val = ((Utf8) val).toString();
        }
        uniqueEntries.get(column).add(getAppropriateType(schema.getDataType(column), val));
      }
    }
  }

  private static Object getAppropriateType(DataType spec, Object val) {
    if (val == null) {
      switch (spec) {
        case DOUBLE:
          return V1Constants.Numbers.NULL_DOUBLE;
        case FLOAT:
          return V1Constants.Numbers.NULL_FLOAT;
        case INT:
          return V1Constants.Numbers.NULL_INT;
        case LONG:
          return V1Constants.Numbers.NULL_LONG;
        default:
          return V1Constants.Str.NULL_STRING;
      }
    }
    return val;
  }

  @Test
  public void test1() throws ConfigurationException, IOException {
    ColumnarSegment heapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.heap);
    ColumnarSegment mmapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.mmap);

    for (String column : heapSegment.getColumnMetadataMap().keySet()) {
      Dictionary<?> heapDictionary = heapSegment.getDictionaryFor(column);
      Dictionary<?> mmapDictionary = mmapSegment.getDictionaryFor(column);

      switch (heapSegment.getColumnMetadataFor(column).getDataType()) {
        case BOOLEAN:
        case STRING:
          Assert.assertEquals(heapDictionary instanceof InMemoryStringDictionary, true);
          Assert.assertEquals(mmapDictionary instanceof MmapStringDictionary, true);
          break;
        case DOUBLE:
          Assert.assertEquals(heapDictionary instanceof InMemoryDoubleDictionary, true);
          Assert.assertEquals(mmapDictionary instanceof MmapDoubleDictionary, true);
          break;
        case FLOAT:
          Assert.assertEquals(heapDictionary instanceof InMemoryFloatDictionary, true);
          Assert.assertEquals(mmapDictionary instanceof MmapFloatDictionary, true);
          break;
        case LONG:
          Assert.assertEquals(heapDictionary instanceof InMemoryLongDictionary, true);
          Assert.assertEquals(mmapDictionary instanceof MmapLongDictionary, true);
          break;
        case INT:
          Assert.assertEquals(heapDictionary instanceof InMemoryIntDictionary, true);
          Assert.assertEquals(mmapDictionary instanceof MmapIntDictionary, true);
          break;
      }
      ;

      Assert.assertEquals(heapDictionary.size(), mmapDictionary.size());
      for (int i = 0; i < heapDictionary.size(); i++) {
        Assert.assertEquals(heapDictionary.getString(i), mmapDictionary.getString(i));
        Assert.assertEquals(heapDictionary.getRaw(i), mmapDictionary.getRaw(i));
      }
    }
  }

  @Test
  public void test2() throws ConfigurationException, IOException {
    ColumnarSegment heapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.heap);
    ColumnarSegment mmapSegment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.mmap);
    Map<String, ColumnMetadata> metadataMap = heapSegment.getColumnMetadataMap();
    for (String column : metadataMap.keySet()) {
      Dictionary<?> heapDictionary = heapSegment.getDictionaryFor(column);
      Dictionary<?> mmapDictionary = mmapSegment.getDictionaryFor(column);
      Set<Object> uniques = uniqueEntries.get(column);
      List<Object> list = Arrays.asList(uniques.toArray());
      Collections.shuffle(list);
      for (Object entry : list) {
        Assert.assertEquals(heapDictionary.indexOf(entry), mmapDictionary.indexOf(entry));
        if (!column.equals("pageKey")) {
          Assert.assertEquals(heapDictionary.indexOf(entry) < 0, false);
          Assert.assertEquals(mmapDictionary.indexOf(entry) < 0, false);
        }
      }
    }
  }
}
