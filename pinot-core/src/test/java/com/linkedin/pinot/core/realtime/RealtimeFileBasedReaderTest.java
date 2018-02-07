/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.realtime.converter.RealtimeSegmentConverter;
import com.linkedin.pinot.core.realtime.impl.FileBasedStreamProviderConfig;
import com.linkedin.pinot.core.realtime.impl.FileBasedStreamProviderImpl;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RealtimeFileBasedReaderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeFileBasedReaderTest.class);

  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static String filePath;
  private static Map<String, FieldType> fieldTypeMap;
  private static Schema schema;
  private static RealtimeSegmentImpl realtimeSegment;
  private static IndexSegment offlineSegment;
  private static final String segmentName = "someSegment";

  private void setUp(SegmentVersion segmentVersion) throws Exception {
    filePath = RealtimeFileBasedReaderTest.class.getClassLoader().getResource(AVRO_DATA).getFile();
    fieldTypeMap = new HashMap<>();
    fieldTypeMap.put("column1", FieldType.DIMENSION);
    fieldTypeMap.put("column2", FieldType.DIMENSION);
    fieldTypeMap.put("column3", FieldType.DIMENSION);
    fieldTypeMap.put("column4", FieldType.DIMENSION);
    fieldTypeMap.put("column5", FieldType.DIMENSION);
    fieldTypeMap.put("column6", FieldType.DIMENSION);
    fieldTypeMap.put("column7", FieldType.DIMENSION);
    fieldTypeMap.put("column8", FieldType.DIMENSION);
    fieldTypeMap.put("column9", FieldType.DIMENSION);
    fieldTypeMap.put("column10", FieldType.DIMENSION);
    fieldTypeMap.put("weeksSinceEpochSunday", FieldType.DIMENSION);
    fieldTypeMap.put("daysSinceEpoch", FieldType.DIMENSION);
    fieldTypeMap.put("column13", FieldType.TIME);
    fieldTypeMap.put("count", FieldType.METRIC);
    schema = SegmentTestUtils.extractSchemaFromAvro(new File(filePath), fieldTypeMap, TimeUnit.MINUTES);

    StreamProviderConfig config = new FileBasedStreamProviderConfig(FileFormat.AVRO, filePath, schema);
    StreamProvider provider = new FileBasedStreamProviderImpl();
    final String tableName = RealtimeFileBasedReaderTest.class.getSimpleName() + ".noTable";
    provider.init(config, tableName, new ServerMetrics(new MetricsRegistry()));

    realtimeSegment = RealtimeSegmentTestUtils.createRealtimeSegmentImpl(schema, 100000, segmentName, AVRO_DATA);
    GenericRow row = provider.next(new GenericRow());
    while (row != null) {
      realtimeSegment.index(row);
      row = provider.next(row);
    }

    provider.shutdown();

    if (new File("/tmp/realtime").exists()) {
      FileUtils.deleteQuietly(new File("/tmp/realtime"));
    }

    RealtimeSegmentConverter conveter =
        new RealtimeSegmentConverter(realtimeSegment, "/tmp/realtime", schema, tableName, segmentName, null);
    conveter.build(segmentVersion, new ServerMetrics(new MetricsRegistry()));

    offlineSegment = Loaders.IndexSegment.load(new File("/tmp/realtime").listFiles()[0], ReadMode.mmap);
  }

  @Test
  public void allTestsV1() throws Exception {
    setUp(SegmentVersion.v1);
    allTests();
  }

  @Test
  public void allTestsV3() throws Exception {
    setUp(SegmentVersion.v3);
    allTests();
  }

  private void allTests() throws Exception {
    testDataSourceWithoutPredicateForSingleValueDimensionColumns();
    testDataSourceWithoutPredicateForSingleValueMetricColumns();
    testDataSourceWithoutPredicateForSingleValueTimeColumns();
    testDataSourceWithoutPredicateForMultiValueDimensionColumns();
  }

  private void testDataSourceWithoutPredicateForSingleValueDimensionColumns() {

    for (FieldSpec spec : schema.getAllFieldSpecs()) {
      if (spec.isSingleValueField() && spec.getFieldType() == FieldType.DIMENSION) {
        DataSource offlineDS = offlineSegment.getDataSource(spec.getName());
        DataSource realtimeDS = realtimeSegment.getDataSource(spec.getName());

        Block offlineBlock = offlineDS.nextBlock();
        Block realtimeBlock = realtimeDS.nextBlock();

        BlockMetadata offlineMetadata = offlineBlock.getMetadata();
        BlockMetadata realtimeMetadata = realtimeBlock.getMetadata();

        BlockSingleValIterator offlineValIterator = (BlockSingleValIterator) offlineBlock.getBlockValueSet().iterator();
        BlockSingleValIterator realtimeValIterator =
            (BlockSingleValIterator) realtimeBlock.getBlockValueSet().iterator();

        Assert.assertEquals(offlineSegment.getSegmentMetadata().getTotalDocs(), realtimeSegment.getNumDocsIndexed());

        while (realtimeValIterator.hasNext()) {
          int offlineDicId = offlineValIterator.nextIntVal();
          int realtimeDicId = realtimeValIterator.nextIntVal();
          try {
            Assert.assertEquals(offlineMetadata.getDictionary().get(offlineDicId),
                realtimeMetadata.getDictionary().get(realtimeDicId));
          } catch (AssertionError e) {
            LOGGER.info("column : {}", spec.getName());
            LOGGER.info("realtimeDicId : {}, rawValue : {}", realtimeDicId,
                realtimeMetadata.getDictionary().get(realtimeDicId));
            LOGGER.info("offlineDicId : {}, rawValue : {}", offlineDicId,
                offlineMetadata.getDictionary().get(offlineDicId));
            throw e;
          }
        }
        Assert.assertEquals(offlineValIterator.hasNext(), realtimeValIterator.hasNext());
      }
    }
  }

  private void testDataSourceWithoutPredicateForSingleValueMetricColumns() {

    for (FieldSpec spec : schema.getAllFieldSpecs()) {
      if (spec.isSingleValueField() && spec.getFieldType() == FieldType.METRIC) {
        DataSource offlineDS = offlineSegment.getDataSource(spec.getName());
        DataSource realtimeDS = realtimeSegment.getDataSource(spec.getName());

        Block offlineBlock = offlineDS.nextBlock();
        Block realtimeBlock = realtimeDS.nextBlock();

        BlockMetadata offlineMetadata = offlineBlock.getMetadata();
        BlockMetadata realtimeMetadata = realtimeBlock.getMetadata();

        BlockSingleValIterator offlineValIterator = (BlockSingleValIterator) offlineBlock.getBlockValueSet().iterator();
        BlockSingleValIterator realtimeValIterator =
            (BlockSingleValIterator) realtimeBlock.getBlockValueSet().iterator();

        Assert.assertEquals(offlineSegment.getSegmentMetadata().getTotalDocs(), realtimeSegment.getNumDocsIndexed());

        while (realtimeValIterator.hasNext()) {
          int offlineDicId = offlineValIterator.nextIntVal();
          int realtimeDicId = realtimeValIterator.nextIntVal();
          Object value;
          if (realtimeMetadata.hasDictionary()) {
            value = realtimeMetadata.getDictionary().get(realtimeDicId);
          } else {
            value = realtimeDicId;
          }
          Assert.assertEquals(offlineMetadata.getDictionary().get(offlineDicId), value);
        }
        Assert.assertEquals(offlineValIterator.hasNext(), realtimeValIterator.hasNext());
      }
    }
  }

  private void testDataSourceWithoutPredicateForSingleValueTimeColumns() {

    for (FieldSpec spec : schema.getAllFieldSpecs()) {
      if (spec.isSingleValueField() && spec.getFieldType() == FieldType.TIME) {
        DataSource offlineDS = offlineSegment.getDataSource(spec.getName());
        DataSource realtimeDS = realtimeSegment.getDataSource(spec.getName());

        Block offlineBlock = offlineDS.nextBlock();
        Block realtimeBlock = realtimeDS.nextBlock();

        BlockMetadata offlineMetadata = offlineBlock.getMetadata();
        BlockMetadata realtimeMetadata = realtimeBlock.getMetadata();

        BlockSingleValIterator offlineValIterator = (BlockSingleValIterator) offlineBlock.getBlockValueSet().iterator();
        BlockSingleValIterator realtimeValIterator =
            (BlockSingleValIterator) realtimeBlock.getBlockValueSet().iterator();

        Assert.assertEquals(offlineSegment.getSegmentMetadata().getTotalDocs(), realtimeSegment.getNumDocsIndexed());

        while (realtimeValIterator.hasNext()) {
          int offlineDicId = offlineValIterator.nextIntVal();
          int realtimeDicId = realtimeValIterator.nextIntVal();
          Assert.assertEquals(offlineMetadata.getDictionary().get(offlineDicId),
              realtimeMetadata.getDictionary().get(realtimeDicId));
        }
        Assert.assertEquals(offlineValIterator.hasNext(), realtimeValIterator.hasNext());
      }
    }
  }

  private void testDataSourceWithoutPredicateForMultiValueDimensionColumns() {

    for (FieldSpec spec : schema.getAllFieldSpecs()) {
      if (!spec.isSingleValueField()) {
        DataSource offlineDS = offlineSegment.getDataSource(spec.getName());
        DataSource realtimeDS = realtimeSegment.getDataSource(spec.getName());

        Block offlineBlock = offlineDS.nextBlock();
        Block realtimeBlock = realtimeDS.nextBlock();

        BlockMetadata offlineMetadata = offlineBlock.getMetadata();
        BlockMetadata realtimeMetadata = realtimeBlock.getMetadata();

        BlockMultiValIterator offlineValIterator = (BlockMultiValIterator) offlineBlock.getBlockValueSet().iterator();
        BlockMultiValIterator realtimeValIterator = (BlockMultiValIterator) realtimeBlock.getBlockValueSet().iterator();
        Assert.assertEquals(offlineSegment.getSegmentMetadata().getTotalDocs(), realtimeSegment.getNumDocsIndexed());

        while (realtimeValIterator.hasNext()) {

          int[] offlineIds = new int[offlineBlock.getMetadata().getMaxNumberOfMultiValues()];
          int[] realtimeIds = new int[realtimeBlock.getMetadata().getMaxNumberOfMultiValues()];

          int Olen = offlineValIterator.nextIntVal(offlineIds);
          int Rlen = realtimeValIterator.nextIntVal(realtimeIds);
          Assert.assertEquals(Olen, Rlen);

          for (int i = 0; i < Olen; i++) {
            Assert.assertEquals(offlineMetadata.getDictionary().get(offlineIds[i]),
                realtimeMetadata.getDictionary().get(realtimeIds[i]));
          }
        }
      }
    }
  }
}
