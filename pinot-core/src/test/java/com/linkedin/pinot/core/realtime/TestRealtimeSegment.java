package com.linkedin.pinot.core.realtime;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdValueIterator;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.realtime.impl.FileBasedStreamProviderConfig;
import com.linkedin.pinot.core.realtime.impl.FileBasedStreamProviderImpl;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;

public class TestRealtimeSegment {
  private static final String AVRO_DATA = "data/mirror-mv.avro";
  private static String filePath;
  private static Map<String, FieldType> fieldTypeMap;
  private static Schema schema;
  private static RealtimeSegment segment;

  @BeforeClass
  public static void before() throws Exception {
    filePath = TestRealtimeFileBasedReader.class.getClassLoader().getResource(AVRO_DATA).getFile();
    fieldTypeMap = new HashMap<String, FieldSpec.FieldType>();
    fieldTypeMap.put("viewerId", FieldType.dimension);
    fieldTypeMap.put("vieweeId", FieldType.dimension);
    fieldTypeMap.put("viewerPrivacySetting", FieldType.dimension);
    fieldTypeMap.put("vieweePrivacySetting", FieldType.dimension);
    fieldTypeMap.put("viewerObfuscationType", FieldType.dimension);
    fieldTypeMap.put("viewerCompanies", FieldType.dimension);
    fieldTypeMap.put("viewerOccupations", FieldType.dimension);
    fieldTypeMap.put("viewerRegionCode", FieldType.dimension);
    fieldTypeMap.put("viewerIndustry", FieldType.dimension);
    fieldTypeMap.put("viewerSchool", FieldType.dimension);
    fieldTypeMap.put("weeksSinceEpochSunday", FieldType.dimension);
    fieldTypeMap.put("daysSinceEpoch", FieldType.dimension);
    fieldTypeMap.put("minutesSinceEpoch", FieldType.time);
    fieldTypeMap.put("count", FieldType.metric);
    schema = SegmentTestUtils.extractSchemaFromAvro(new File(filePath), fieldTypeMap, TimeUnit.MINUTES);

    StreamProviderConfig config = new FileBasedStreamProviderConfig(FileFormat.AVRO, filePath, schema);
    System.out.println(config);
    StreamProvider provider = new FileBasedStreamProviderImpl();
    provider.init(config);

    segment = new RealtimeSegmentImpl(schema);
    GenericRow row = provider.next();
    while (row != null) {
      segment.index(row);
      row = provider.next();
    }
    provider.shutdown();
  }

  @Test
  public void test1() throws Exception {
    DataSource ds = segment.getDataSource("viewerId");
    Block b = ds.nextBlock();
    BlockValSet set = b.getBlockValueSet();
    BlockSingleValIterator it = (BlockSingleValIterator) set.iterator();
    BlockMetadata metadata = b.getMetadata();
    while (it.next()) {
      int dicId = it.nextIntVal();
    }

  }
}
