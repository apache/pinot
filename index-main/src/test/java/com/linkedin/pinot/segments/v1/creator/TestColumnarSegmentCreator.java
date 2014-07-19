package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.pinot.index.block.intarray.CompressedIntArrayBlock;
import com.linkedin.pinot.index.block.intarray.CompressedIntArrayDataSource;
import com.linkedin.pinot.index.common.BlockDocIdIterator;
import com.linkedin.pinot.index.common.BlockDocIdValueIterator;
import com.linkedin.pinot.index.common.BlockValIterator;
import com.linkedin.pinot.index.common.Constants;
import com.linkedin.pinot.index.common.Predicate;
import com.linkedin.pinot.index.common.Predicate.Type;
import com.linkedin.pinot.index.data.FieldSpec;
import com.linkedin.pinot.index.data.FieldSpec.FieldType;
import com.linkedin.pinot.index.data.Schema;
import com.linkedin.pinot.index.time.SegmentTimeUnit;
import com.linkedin.pinot.raw.record.readers.FileFormat;
import com.linkedin.pinot.raw.record.readers.RecordReaderFactory;
import com.linkedin.pinot.segments.creator.SegmentCreator;
import com.linkedin.pinot.segments.creator.SegmentCreatorFactory;
import com.linkedin.pinot.segments.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.segments.generator.SegmentVersion;
import com.linkedin.pinot.segments.v1.segment.ColumnMetadata;
import com.linkedin.pinot.segments.v1.segment.ColumnarSegment;
import com.linkedin.pinot.segments.v1.segment.SegmentLoader;
import com.linkedin.pinot.segments.v1.segment.SegmentLoader.IO_MODE;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.utils.HeapCompressedIntArray;
import com.linkedin.pinot.segments.v1.segment.utils.Helpers;
import com.linkedin.pinot.segments.v1.segment.utils.IntArray;


public class TestColumnarSegmentCreator {
  private final String AVRO_DATA = "data/sample_data.avro";
  private final String JSON_DATA = "data/sample_data.json";
  private final String AVRO_MULTI_DATA = "data/sample_data_multi_value.avro";
  private static File INDEX_DIR = new File("V1_INDEX_DIR");
  private List<String> allColumns;

  @Test
  public void test1() throws ConfigurationException, IOException {
    ColumnarSegment segment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.heap);

    // simple not null checks
    for (String column : allColumns) {
      Assert.assertNotNull(segment.getIntArrayFor(column));
      Assert.assertNotNull(segment.getDictionaryFor(column));
      Assert.assertNotNull(segment.getMetadataFor(column));
      Assert.assertNotNull(segment.getDataSource(column));
    }

    // now lets get fancy
    for (String column : allColumns) {
      CompressedIntArrayDataSource ds = (CompressedIntArrayDataSource) segment.getDataSource(column);
      CompressedIntArrayBlock block = (CompressedIntArrayBlock) ds.nextBlock();

      // you should apply a predicate before you access the iterator

      BlockDocIdIterator it = block.getBlockDocIdSet().iterator();
      int docId = it.next();
      while (docId != Constants.EOF) {
        docId = it.next();
      }

      BlockDocIdValueIterator it1 = block.getBlockDocIdValueSet().iterator();
      while (it1.advance()) {
        //System.out.println(it1.currentDocId() + ":" + it1.currentVal());
      }

      BlockValIterator it2 = block.getBlockValueSet().iterator();
      int valId = it2.nextVal();
      while (valId != Constants.EOF) {
        //System.out.println(valId);
        valId = it2.nextVal();
      }
      break;
    }
  }

  @Test
  public void test2() throws ConfigurationException, IOException {
    ColumnarSegment segment = (ColumnarSegment) SegmentLoader.load(INDEX_DIR, IO_MODE.heap);

    List<String> rhs = new ArrayList<String>();
    rhs.add("4625");
    Predicate p = new Predicate("dim_memberRegion", Predicate.Type.EQ, rhs);
    CompressedIntArrayDataSource ds = (CompressedIntArrayDataSource) segment.getDataSource("dim_memberRegion", p);
    ds.setPredicate(p);
    CompressedIntArrayBlock b = (CompressedIntArrayBlock) ds.nextBlock();

    BlockDocIdIterator it = b.getBlockDocIdSet().iterator();
    int index = it.next();
    while (index != Constants.EOF) {
      System.out.println(index);
      index = it.next();
    }
  }

  @Test
  public void test3() throws Exception {
    File baseDir = new File("/home/dpatel/data/demoData");
    for (int i = 0; i < 50; i++) {
      SegmentGeneratorConfiguration config = getConfigs();
      config.setResourceName("testResource");
      config.setTableName("testTable");
      File f = new File(baseDir, "segment-" + i);
      config.setOutputDir(f.getAbsolutePath());
      SegmentCreator creator = SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
      creator.init(config);
      creator.buildSegment();
    }
  }

  public SegmentGeneratorConfiguration getConfigs() {
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();
    allColumns = new ArrayList<String>();

    allColumns.add("shrd_advertiserId");
    allColumns.add("sort_campaignId");
    allColumns.add("dim_campaignType");
    allColumns.add("dim_creativeId");
    allColumns.add("dim_requestTypeInt");
    allColumns.add("time_day");
    allColumns.add("dim_memberAge");
    allColumns.add("dim_memberCompany");
    allColumns.add("dim_memberEducation");
    allColumns.add("dim_memberFunction");
    allColumns.add("dim_memberGender");
    allColumns.add("dim_memberIndustry");
    allColumns.add("dim_memberRegion");
    allColumns.add("dim_memberSeniority");
    allColumns.add("dim_memberTitles");
    allColumns.add("met_impressionCount");

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
      if (column.startsWith("time")) {
        spec = new FieldSpec(column, FieldType.time, null, true);
      } else if (column.startsWith("met")) {
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

  @Before
  public void setup() throws Exception {
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();
    allColumns = new ArrayList<String>();

    allColumns.add("shrd_advertiserId");
    allColumns.add("sort_campaignId");
    allColumns.add("dim_campaignType");
    allColumns.add("dim_creativeId");
    allColumns.add("dim_requestTypeInt");
    allColumns.add("time_day");
    allColumns.add("dim_memberAge");
    allColumns.add("dim_memberCompany");
    allColumns.add("dim_memberEducation");
    allColumns.add("dim_memberFunction");
    allColumns.add("dim_memberGender");
    allColumns.add("dim_memberIndustry");
    allColumns.add("dim_memberRegion");
    allColumns.add("dim_memberSeniority");
    allColumns.add("dim_memberTitles");
    allColumns.add("met_impressionCount");

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
      if (column.startsWith("time")) {
        spec = new FieldSpec(column, FieldType.time, null, true);
      } else if (column.startsWith("met")) {
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
