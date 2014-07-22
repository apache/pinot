package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
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
  private static File INDEX_DIR = new File("V1_INDEX_DIR");
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

  public SegmentGeneratorConfiguration getConfigs() {
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();
    allColumns = new ArrayList<String>();

    allColumns.add("pageKey");
    allColumns.add("hoursSinceEpoch");
    allColumns.add("daysSinceEpoch");
    allColumns.add("fabric");
    allColumns.add("type");
    allColumns.add("logged");
    allColumns.add("os");
    allColumns.add("device");
    allColumns.add("version");
    allColumns.add("count");

    SegmentGeneratorConfiguration config = new SegmentGeneratorConfiguration();
    config.setFileFormat(FileFormat.avro);
    config.setFilePath(filePath);
    config.setProjectedColumns(allColumns);
    config.setSegmentVersion(SegmentVersion.v1);

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    Schema schema = new Schema();
    for (String column : allColumns) {
      FieldSpec spec;
      if (column.startsWith("daysSinceEpoch")) {
        spec = new FieldSpec(column, FieldType.time, null, true);
      } else if (column.startsWith("count")) {
        spec = new FieldSpec(column, FieldType.metric, null, true);
      } else {
        spec = new FieldSpec(column, FieldType.dimension, null, true);
      }
      schema.addSchema(column, spec);
    }

    config.setSchema(schema);
    config.setSegmentTimeUnit(SegmentTimeUnit.days);
    return config;
  }

  @BeforeTest
  public void setup() throws Exception {
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();
    allColumns = new ArrayList<String>();

    allColumns.add("pageKey");
    allColumns.add("hoursSinceEpoch");
    allColumns.add("daysSinceEpoch");
    allColumns.add("fabric");
    allColumns.add("type");
    allColumns.add("logged");
    allColumns.add("os");
    allColumns.add("device");
    allColumns.add("version");
    allColumns.add("count");

    SegmentGeneratorConfiguration config = new SegmentGeneratorConfiguration();
    config.setFileFormat(FileFormat.avro);
    config.setFilePath(filePath);
    config.setProjectedColumns(allColumns);
    config.setSegmentVersion(SegmentVersion.v1);

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    Schema schema = new Schema();
    for (String column : allColumns) {
      FieldSpec spec;
      if (column.startsWith("daysSinceEpoch")) {
        spec = new FieldSpec(column, FieldType.time, null, true);
      } else if (column.startsWith("count")) {
        spec = new FieldSpec(column, FieldType.metric, null, true);
      } else {
        spec = new FieldSpec(column, FieldType.dimension, null, true);
      }
      schema.addSchema(column, spec);
    }

    config.setSchema(schema);
    config.setSegmentTimeUnit(SegmentTimeUnit.days);
    config.setOutputDir(INDEX_DIR.getAbsolutePath());

    ColumnarSegmentCreator creator =
        (ColumnarSegmentCreator) SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
    creator.init(config);
    creator.buildSegment();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }
}

/*
 * {"name":"shrd_advertiserId","type":["null","long"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"sort_campaignId","type":["null","int"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"dim_campaignType","type":["null","string"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"dim_creativeId","type":["null","int"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"dim_requestTypeInt","type":["null","int"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"time_day","type":["null","int"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"dim_memberAge","type":["null","string"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"dim_memberCompany","type":["null","string"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"dim_memberEducation","type":["null","string"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"dim_memberFunction","type":["null","string"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"dim_memberGender","type":["null","string"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"dim_memberIndustry","type":["null","string"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"dim_memberRegion","type":["null","string"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"dim_memberSeniority","type":["null","string"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"dim_memberTitles","type":["null","string"],"doc":"autogenerated from Pig Field Schema"},
 * {"name":"met_impressionCount","type":["null","long"],"doc":"autogenerated from Pig Field Schema"}
 * */
