package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnMetadata;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.columnar.creator.ColumnarSegmentCreator;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreatorFactory;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryDoubleDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryFloatDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryIntDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryLongDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryStringDictionary;
import com.linkedin.pinot.core.indexsegment.generator.ChunkGeneratorConfiguration;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.time.SegmentTimeUnit;


public class TestSegment {
  private final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File(TestSegment.class.toString());

  @Test
  public void test1() throws ConfigurationException, IOException {
    ColumnarSegment segment = (ColumnarSegment) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
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
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public void setup() throws Exception {
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    ChunkGeneratorConfiguration config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            SegmentTimeUnit.days, "test", "testTable");

    ColumnarSegmentCreator creator =
        (ColumnarSegmentCreator) SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
    creator.init(config);
    creator.buildSegment();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }
}
