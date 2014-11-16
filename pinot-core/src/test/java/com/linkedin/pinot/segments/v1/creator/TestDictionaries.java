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
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnMetadata;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreator;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreatorFactory;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryDoubleDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryFloatDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryIntDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryLongDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryStringDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.mmap.MmapDoubleDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.mmap.MmapFloatDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.mmap.MmapIntDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.mmap.MmapLongDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.mmap.MmapStringDictionary;
import com.linkedin.pinot.core.indexsegment.generator.ChunkGeneratorConfiguration;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.time.SegmentTimeUnit;


public class TestDictionaries {
  private static final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File(TestDictionaries.class.toString());
  static Map<String, Set<Object>> uniqueEntries;

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public static void before() throws Exception {
    String filePath = TestDictionaries.class.getClassLoader().getResource(AVRO_DATA).getFile();
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    ChunkGeneratorConfiguration config =
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

    uniqueEntries = new HashMap<String, Set<Object>>();
    for (String column : columns) {
      uniqueEntries.put(column, new HashSet<Object>());
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
    ColumnarSegment heapSegment = (ColumnarSegment) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
    ColumnarSegment mmapSegment = (ColumnarSegment) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.mmap);

    for (String column : heapSegment.getColumnMetadataMap().keySet()) {
      Dictionary<?> heapDictionary = heapSegment.getDictionaryFor(column);
      Dictionary<?> mmapDictionary = mmapSegment.getDictionaryFor(column);

      switch (heapSegment.getColumnMetadataFor(column).getDataType()) {
        case BOOLEAN:
        case STRING:
          AssertJUnit.assertEquals(heapDictionary instanceof InMemoryStringDictionary, true);
          AssertJUnit.assertEquals(mmapDictionary instanceof MmapStringDictionary, true);
          break;
        case DOUBLE:
          AssertJUnit.assertEquals(heapDictionary instanceof InMemoryDoubleDictionary, true);
          AssertJUnit.assertEquals(mmapDictionary instanceof MmapDoubleDictionary, true);
          break;
        case FLOAT:
          AssertJUnit.assertEquals(heapDictionary instanceof InMemoryFloatDictionary, true);
          AssertJUnit.assertEquals(mmapDictionary instanceof MmapFloatDictionary, true);
          break;
        case LONG:
          AssertJUnit.assertEquals(heapDictionary instanceof InMemoryLongDictionary, true);
          AssertJUnit.assertEquals(mmapDictionary instanceof MmapLongDictionary, true);
          break;
        case INT:
          AssertJUnit.assertEquals(heapDictionary instanceof InMemoryIntDictionary, true);
          AssertJUnit.assertEquals(mmapDictionary instanceof MmapIntDictionary, true);
          break;
      }

      AssertJUnit.assertEquals(heapDictionary.size(), mmapDictionary.size());
      for (int i = 0; i < heapDictionary.size(); i++) {
        AssertJUnit.assertEquals(heapDictionary.getString(i), mmapDictionary.getString(i));
        AssertJUnit.assertEquals(heapDictionary.getRaw(i), mmapDictionary.getRaw(i));
      }
    }
  }

  @Test
  public void test2() throws ConfigurationException, IOException {
    ColumnarSegment heapSegment = (ColumnarSegment) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
    ColumnarSegment mmapSegment = (ColumnarSegment) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.mmap);
    Map<String, ColumnMetadata> metadataMap = heapSegment.getColumnMetadataMap();
    for (String column : metadataMap.keySet()) {
      Dictionary<?> heapDictionary = heapSegment.getDictionaryFor(column);
      Dictionary<?> mmapDictionary = mmapSegment.getDictionaryFor(column);
      Set<Object> uniques = uniqueEntries.get(column);
      List<Object> list = Arrays.asList(uniques.toArray());
      Collections.shuffle(list);
      for (Object entry : list) {
        AssertJUnit.assertEquals(heapDictionary.indexOf(entry), mmapDictionary.indexOf(entry));
        if (!column.equals("pageKey")) {
          AssertJUnit.assertEquals(heapDictionary.indexOf(entry) < 0, false);
          AssertJUnit.assertEquals(mmapDictionary.indexOf(entry) < 0, false);
        }
      }
    }
  }
}
