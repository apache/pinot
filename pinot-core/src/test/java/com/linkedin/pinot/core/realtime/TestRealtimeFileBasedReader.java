package com.linkedin.pinot.core.realtime;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.realtime.converter.RealtimeSegmentConverter;
import com.linkedin.pinot.core.realtime.impl.FileBasedStreamProviderConfig;
import com.linkedin.pinot.core.realtime.impl.FileBasedStreamProviderImpl;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


public class TestRealtimeFileBasedReader {

  private static final String AVRO_DATA = "data/mirror-mv.avro";
  private static String filePath;
  private static Map<String, FieldType> fieldTypeMap;
  private static Schema schema;
  private static RealtimeSegmentImpl realtimeSegment;
  private static IndexSegment offlineSegment;

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
    StreamProvider provider = new FileBasedStreamProviderImpl();
    provider.init(config);

    realtimeSegment = new RealtimeSegmentImpl(schema);
    GenericRow row = provider.next();
    while (row != null) {
      realtimeSegment.index(row);
      row = provider.next();
    }

    provider.shutdown();

    if (new File("/tmp/realtime").exists()) {
      FileUtils.deleteQuietly(new File("/tmp/realtime"));
    }

    RealtimeSegmentConverter conveter =
        new RealtimeSegmentConverter(realtimeSegment, "/tmp/realtime", schema, "mirror", "mirror");
    conveter.build();

    offlineSegment = Loaders.IndexSegment.load(new File("/tmp/realtime").listFiles()[0], ReadMode.mmap);
  }

  @Test
  public void testDataSourceWithoutPredicates() {
    DataSource offlineDS = offlineSegment.getDataSource("viewerId");
    DataSource realtimeDS = realtimeSegment.getDataSource("viewerId");

    Block offlineBlock = offlineDS.nextBlock();
    Block realtimeBlock = realtimeDS.nextBlock();

    BlockMetadata offlineMetadata = offlineBlock.getMetadata();
    BlockMetadata realtimeMetadata = realtimeBlock.getMetadata();

    BlockSingleValIterator offlineValIterator = (BlockSingleValIterator) offlineBlock.getBlockValueSet().iterator();
    BlockSingleValIterator realtimeValIterator = (BlockSingleValIterator) realtimeBlock.getBlockValueSet().iterator();

    Assert.assertEquals(offlineSegment.getSegmentMetadata().getTotalDocs(),
        realtimeSegment.getNumberOfDocIds());

    while (realtimeValIterator.hasNext()) {
      int offlineDicId = offlineValIterator.nextIntVal();
      int realtimeDicId = realtimeValIterator.nextIntVal();

      Assert.assertEquals(offlineMetadata.getDictionary().get(offlineDicId),
          realtimeMetadata.getDictionary().get(realtimeDicId));

    }
  }
}
