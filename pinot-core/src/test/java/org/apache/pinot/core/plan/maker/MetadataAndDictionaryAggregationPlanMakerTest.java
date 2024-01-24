/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.plan.maker;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.FastFilteredCountOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.operator.query.NonScanBasedAggregationOperator;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.upsert.ConcurrentMapPartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class MetadataAndDictionaryAggregationPlanMakerTest {
  private static final String AVRO_DATA = "data" + File.separator + "test_data-sv.avro";
  private static final String SEGMENT_NAME = "testTable_201711219_20171120";
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "MetadataAndDictionaryAggregationPlanMakerTest");
  private static final InstancePlanMakerImplV2 PLAN_MAKER = new InstancePlanMakerImplV2();

  private IndexSegment _indexSegment;
  private IndexSegment _upsertIndexSegment;

  @BeforeTest
  public void buildSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = resource.getFile();

    // Build the segment schema.
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addMetric("column1", FieldSpec.DataType.INT)
        .addMetric("column3", FieldSpec.DataType.INT).addSingleValueDimension("column5", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column6", FieldSpec.DataType.INT)
        .addSingleValueDimension("column7", FieldSpec.DataType.INT)
        .addSingleValueDimension("column9", FieldSpec.DataType.INT)
        .addSingleValueDimension("column11", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column12", FieldSpec.DataType.STRING).addMetric("column17", FieldSpec.DataType.INT)
        .addMetric("column18", FieldSpec.DataType.INT)
        .addTime(new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"), null).build();
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setSegmentTimeValueCheck(false);
    ingestionConfig.setRowTimeValueCheck(false);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("daysSinceEpoch")
            .setIngestionConfig(ingestionConfig).build();

    // Create the segment generator config.
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setIndexOn(StandardIndexes.inverted(), IndexConfig.ENABLED, "column6", "column7", "column11",
        "column17", "column18");

    // Build the index segment.
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
  }

  @BeforeClass
  public void loadSegment()
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));
    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.heap);
    _upsertIndexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.heap);
    UpsertContext upsertContext =
        new UpsertContext.Builder().setTableConfig(mock(TableConfig.class)).setSchema(mock(Schema.class))
            .setPrimaryKeyColumns(Collections.singletonList("column6"))
            .setComparisonColumns(Collections.singletonList("daysSinceEpoch")).setTableIndexDir(INDEX_DIR).build();
    ConcurrentMapPartitionUpsertMetadataManager upsertMetadataManager =
        new ConcurrentMapPartitionUpsertMetadataManager("testTable_REALTIME", 0, upsertContext);
    ((ImmutableSegmentImpl) _upsertIndexSegment).enableUpsert(upsertMetadataManager,
        new ThreadSafeMutableRoaringBitmap(), null);
  }

  @AfterClass
  public void destroySegment() {
    _indexSegment.destroy();
  }

  @AfterTest
  public void deleteSegment() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test(dataProvider = "testPlanMakerDataProvider")
  public void testPlanMaker(String query, Class<? extends Operator<?>> operatorClass,
      Class<? extends Operator<?>> upsertOperatorClass) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    Operator<?> operator = PLAN_MAKER.makeSegmentPlanNode(_indexSegment, queryContext).run();
    assertTrue(operatorClass.isInstance(operator));
    Operator<?> upsertOperator = PLAN_MAKER.makeSegmentPlanNode(_upsertIndexSegment, queryContext).run();
    assertTrue(upsertOperatorClass.isInstance(upsertOperator));
  }

  @DataProvider(name = "testPlanMakerDataProvider")
  public Object[][] testPlanMakerDataProvider() {
    List<Object[]> entries = new ArrayList<>();
    // Selection
    entries.add(new Object[]{
        "select * from testTable", SelectionOnlyOperator.class, SelectionOnlyOperator.class
    });
    // Selection
    entries.add(new Object[]{
        "select column1,column5 from testTable", SelectionOnlyOperator.class, SelectionOnlyOperator.class
    });
    // Selection with filter
    entries.add(new Object[]{
        "select * from testTable where daysSinceEpoch > 100", SelectionOnlyOperator.class, SelectionOnlyOperator.class
    });
    // COUNT from metadata (via fast filtered count which knows the number of docs in the segment)
    entries.add(new Object[]{
        "select count(*) from testTable", FastFilteredCountOperator.class, FastFilteredCountOperator.class
    });
    // COUNT from metadata with match all filter
    entries.add(new Object[]{
        "select count(*) from testTable where column1 > 10", FastFilteredCountOperator.class,
        FastFilteredCountOperator.class
    });
    // MIN/MAX from dictionary
    entries.add(new Object[]{
        "select max(daysSinceEpoch),min(daysSinceEpoch) from testTable", NonScanBasedAggregationOperator.class,
        AggregationOperator.class
    });
    // MIN/MAX from dictionary with match all filter
    entries.add(new Object[]{
        "select max(daysSinceEpoch),min(daysSinceEpoch) from testTable where column1 > 10",
        NonScanBasedAggregationOperator.class, AggregationOperator.class
    });
    // MINMAXRANGE from dictionary
    entries.add(new Object[]{
        "select minmaxrange(daysSinceEpoch) from testTable", NonScanBasedAggregationOperator.class,
        AggregationOperator.class
    });
    // MINMAXRANGE from dictionary with match all filter
    entries.add(new Object[]{
        "select minmaxrange(daysSinceEpoch) from testTable where column1 > 10", NonScanBasedAggregationOperator.class,
        AggregationOperator.class
    });
    // Aggregation
    entries.add(new Object[]{
        "select sum(column1) from testTable", AggregationOperator.class, AggregationOperator.class
    });
    // Aggregation group-by
    entries.add(new Object[]{
        "select sum(column1) from testTable group by daysSinceEpoch", GroupByOperator.class, GroupByOperator.class
    });
    // COUNT from metadata, MIN from dictionary
    entries.add(new Object[]{
        "select count(*),min(column17) from testTable", NonScanBasedAggregationOperator.class, AggregationOperator.class
    });
    // Aggregation group-by
    entries.add(new Object[]{
        "select count(*),min(daysSinceEpoch) from testTable group by daysSinceEpoch", GroupByOperator.class,
        GroupByOperator.class
    });

    return entries.toArray(new Object[entries.size()][]);
  }
}
