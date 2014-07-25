package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.index.data.FieldSpec;
import com.linkedin.pinot.index.data.FieldSpec.FieldType;
import com.linkedin.pinot.index.data.Schema;
import com.linkedin.pinot.index.time.SegmentTimeUnit;
import com.linkedin.pinot.raw.record.readers.FileFormat;
import com.linkedin.pinot.raw.record.readers.RecordReaderFactory;
import com.linkedin.pinot.segments.creator.SegmentCreatorFactory;
import com.linkedin.pinot.segments.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.segments.generator.SegmentVersion;
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


public class TestColumnarSegmentCreator {
  private final String AVRO_DATA = "data/sample_pv_data.avro";
  private static File INDEX_DIR = new File(TestColumnarSegmentCreator.class.toString());
  private List<String> allColumns;

  @Test
  public void test1() throws ConfigurationException, IOException {
    ColumnarSegment segment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.heap);
    Map<String, Dictionary<?>> dictionaryMap = segment.getDictionaryMap();
    Map<String, ColumnMetadata> medataMap = segment.getColumnMetadataMap();

    for (String column : dictionaryMap.keySet()) {
      Dictionary<?> dic = dictionaryMap.get(column);
      if (dic instanceof InMemoryStringDictionary) {
        System.out.println(column + " : " + "String " + " : " + medataMap.get(column).getDataType());
      } else if (dic instanceof InMemoryIntDictionary) {
        System.out.println(column + " : " + "Integer " + " : " + medataMap.get(column).getDataType());
      } else if (dic instanceof InMemoryLongDictionary) {
        System.out.println(column + " : " + "Long " + " : " + medataMap.get(column).getDataType());
      } else if (dic instanceof InMemoryFloatDictionary) {
        System.out.println(column + " : " + "Float " + " : " + medataMap.get(column).getDataType());
      } else if (dic instanceof InMemoryDoubleDictionary) {
        System.out.println(column + " : " + "Double " + " : " + medataMap.get(column).getDataType());
      }
    }
  }

  @AfterClass
  public void teardown() {
    //FileUtils.deleteQuietly(INDEX_DIR);
  }
  
  @BeforeClass
  public void setup() throws Exception {
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEX_DIR.exists())
      FileUtils.deleteQuietly(INDEX_DIR);

    SegmentGeneratorConfiguration config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            SegmentTimeUnit.days, "test", "testTable");

    ColumnarSegmentCreator creator =
        (ColumnarSegmentCreator) SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
    creator.init(config);
    creator.buildSegment();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }
}
