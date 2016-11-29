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
package com.linkedin.pinot.queries;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator.TestSimpleAggreationQuery;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.manager.config.FileBasedInstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.FileBasedInstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.startree.hll.HllConfig;
import com.linkedin.pinot.core.startree.hll.HllConstants;
import com.linkedin.pinot.core.startree.hll.HllUtil;
import com.linkedin.pinot.core.startree.hll.SegmentWithHllIndexCreateHelper;
import com.linkedin.pinot.util.TestUtils;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Served the same purpose as QueriesSentinelTest, but extracted out as a separate class to make the logic clearer.
 * In minor places, the logic is not the same as QueriesSentinelTest.
 * E.g. different data loaded, pre-assigned query columns, etc.
 */
public class HllIndexSentinelTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(HllIndexSentinelTest.class);

  private static final String timeColumnName = "daysSinceEpoch";
  private static final TimeUnit timeUnit = TimeUnit.DAYS;
  private static final String hllDeriveColumnSuffix = HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX;
  private static final int hllLog2m = HllConstants.DEFAULT_LOG2M;
  private static final double approximationThreshold = 0.001;
  private static final String tableName = "testTable";

  private static QueryExecutor QUERY_EXECUTOR;
  private static FileBasedInstanceDataManager instanceDataManager;
  private static PropertiesConfiguration serverConf;
  private SegmentWithHllIndexCreateHelper helper;
  private String segmentName;
  private  ServerMetrics serverMetrics;

  private static final String AVRO_DATA = "data/test_data-sv.avro";

  private static final Set<String> columnsToDeriveHllFields =
      new HashSet<>(Arrays.asList("column1", "column2", "column3",
          "count", "weeksSinceEpochSunday", "daysSinceEpoch",
          "column17", "column18"));

  private static final HllConfig hllConfig =
      new HllConfig(hllLog2m, columnsToDeriveHllFields, hllDeriveColumnSuffix);

  private void setupTableManager() throws Exception {
    serverMetrics = new ServerMetrics(new MetricsRegistry());
    TableDataManagerProvider.setServerMetrics(serverMetrics);

    serverConf =  new TestingServerPropertiesBuilder(tableName).build();
    serverConf.setDelimiterParsingDisabled(false);

    instanceDataManager = FileBasedInstanceDataManager.getInstanceDataManager();
    instanceDataManager.init(new FileBasedInstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager.start();
  }

  @BeforeClass
  public void setup() throws Exception {
    setupTableManager();
    // Setup Segment
    helper = new SegmentWithHllIndexCreateHelper(tableName, AVRO_DATA, timeColumnName, timeUnit, "starTreeSegment");
    SegmentIndexCreationDriver driver = helper.build(true, hllConfig);
    File segmentFile = helper.getSegmentDirectory();
    segmentName = helper.getSegmentName();
    LOGGER.debug("************************** Segment Directory: " + segmentFile.getAbsolutePath());

    // Load Segment
    final IndexSegment indexSegment = ColumnarSegmentLoader.load(segmentFile, ReadMode.heap);
    instanceDataManager.getTableDataManager(tableName).addSegment(indexSegment);

    // Init Query Executor
    QUERY_EXECUTOR = new ServerQueryExecutorV1Impl(false);
    QUERY_EXECUTOR.init(serverConf.subset("pinot.server.query.executor"), instanceDataManager, new ServerMetrics(
        new MetricsRegistry()));

  }

  @AfterClass
  public void tearDown() {
//    helper.cleanTempDir();
  }

  @Test
  public void testFastHllNoGroupBy() throws Exception {
    final int baseValue = 10000000;
    final String[] filterColumns = {"column1" /* first split */, "column17" /* low priority in split */};

    for (String filterColumn: filterColumns) {
      for (String distinctCountColumn : columnsToDeriveHllFields) {
        final List<TestSimpleAggreationQuery> aggCalls = new ArrayList<>();
        aggCalls.add(new TestSimpleAggreationQuery(
            "select fasthll(" + distinctCountColumn + ") from " + tableName +
                " where " + filterColumn + " > " + baseValue + " limit 0",
            0.0));
        aggCalls.add(new TestSimpleAggreationQuery(
            "select distinctcounthll(" + distinctCountColumn + ") from " + tableName +
                " where " + filterColumn + " > " + baseValue + " limit 0",
            0.0));
        ApproximateQueryTestUtil.runApproximationQueries(
            QUERY_EXECUTOR, segmentName, aggCalls, approximationThreshold, serverMetrics);

        // correct query
        Object ret = ApproximateQueryTestUtil.runQuery(
            QUERY_EXECUTOR, segmentName, new TestSimpleAggreationQuery(
                "select distinctcount(" + distinctCountColumn + ") from " + tableName +
                    " where " + filterColumn + " > " + baseValue + " limit 0",
                0.0), serverMetrics);
        LOGGER.debug(ret.toString());
      }
    }
  }

  @Test
  public void testFastHllWithGroupBy() throws Exception {
    final int baseValue = 10000000;
    final String[] filterColumns = {"column1" /* first split */, "column17" /* low priority in split */};

    // === info about data/test_data-sv.avro data ===
    // column17: Int, cardinality: 25, has index built
    // column13: String, cardinality: 6, no hll index built
    // column1: Int, cardinality: 6583, has hll index built
    // column9: Int, cardinality: 1738, no hll index built
    final String[] gbyColumns = new String[]{"column17", "column13", "column1", "column9"};

    for (String filterColumn: filterColumns) {
      for (String gbyColumn : gbyColumns) {
        for (String distinctCountColumn : columnsToDeriveHllFields) {
          final List<AvroQueryGenerator.TestGroupByAggreationQuery> groupByCalls = new ArrayList<>();
          groupByCalls.add(new AvroQueryGenerator.TestGroupByAggreationQuery(
              "select fasthll(" + distinctCountColumn + ") from " + tableName +
                  " where " + filterColumn + " < " + baseValue +
                  " group by " + gbyColumn + " limit 0", null));
          groupByCalls.add(new AvroQueryGenerator.TestGroupByAggreationQuery(
              "select distinctcounthll(" + distinctCountColumn + ") from " + tableName +
                  " where " + filterColumn + " < " + baseValue +
                  " group by " + gbyColumn + " limit 0", null));
          ApproximateQueryTestUtil.runApproximationQueries(
              QUERY_EXECUTOR, segmentName, groupByCalls, approximationThreshold, serverMetrics);

          // correct query
          Object ret = ApproximateQueryTestUtil.runQuery(
              QUERY_EXECUTOR, segmentName, new AvroQueryGenerator.TestGroupByAggreationQuery(
                  "select distinctcount(" + distinctCountColumn + ") from " + tableName +
                      " where " + filterColumn + " < " + baseValue +
                      " group by " + gbyColumn + " limit 0", null), serverMetrics);
          LOGGER.debug(ret.toString());
        }
      }
    }
  }

  @Test
  public void testHllOnPregeneratedColumn()
      throws Exception {
    // First read avro file and generate avro file with an hll column

    File newAvroDir = Files.createTempDirectory(HllIndexSentinelTest.class.getName() + "_PregeneratedHllAvro").toFile();
    File newAvroFile = new File(newAvroDir, "data.avro");
    DataFileStream<GenericRecord> avroReader = null;
    try {
      String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));
      avroReader = AvroUtils.getAvroReader(new File(filePath));
      Schema currentSchema = avroReader.getSchema();
      List<Schema.Field> fields = currentSchema.getFields();
      final String hllColumnName = "column1Hll";
      final int log2m = 12;
      List<Schema.Field> newFieldList =  new ArrayList<>(fields.size());
      for (Schema.Field field : fields) {
        newFieldList.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()));
      }
      newFieldList.add(new Schema.Field(hllColumnName, Schema.create(Schema.Type.STRING), null, null));
      Schema updatedSchema = Schema.createRecord(newFieldList);

      try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<GenericData.Record>(new GenericDatumWriter<GenericData.Record>(updatedSchema))) {
        writer.create(updatedSchema, newAvroFile);
        while (avroReader.hasNext()) {
          GenericRecord record = avroReader.next();
          GenericData.Record newRecord = new GenericData.Record(updatedSchema);
          for (Schema.Field field : fields) {
            newRecord.put(field.name(),record.get(field.name()));
          }
          newRecord.put(hllColumnName, HllUtil.singleValueHllAsString(log2m, record.get("column1")));
          writer.append(newRecord);
        }
      }

      SegmentWithHllIndexCreateHelper segmentGenHelper =
          new SegmentWithHllIndexCreateHelper(tableName, AVRO_DATA, timeColumnName, timeUnit, "pregeneratedSegment");
      SegmentIndexCreationDriver driver = segmentGenHelper.build(true, hllConfig);
      File segmentFile = segmentGenHelper.getSegmentDirectory();
      String newSegmentName = segmentGenHelper.getSegmentName();
      LOGGER.debug("************************** Segment Directory: " + segmentFile.getAbsolutePath());

      // Load Segment
      final IndexSegment indexSegment = ColumnarSegmentLoader.load(segmentFile, ReadMode.mmap);
      instanceDataManager.getTableDataManager(tableName).addSegment(indexSegment);

      // Init Query Executor
      final List<TestSimpleAggreationQuery> aggCalls = new ArrayList<>();
        aggCalls.add(new TestSimpleAggreationQuery(
            "select fasthll(column1) from " + tableName +
                " where column1 > 100000 limit 0",
            0.0));
        aggCalls.add(new TestSimpleAggreationQuery(
             "select distinctcounthll(column1) from " + tableName +
                " where column1 > 100000 limit 0",
            0.0));
        ApproximateQueryTestUtil.runApproximationQueries(
            QUERY_EXECUTOR, newSegmentName, aggCalls, approximationThreshold, serverMetrics);

    } finally {
      if (avroReader != null) {
        avroReader.close();
      }
    }
  }
}
