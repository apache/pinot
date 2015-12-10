/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.query.aggregation;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.query.MAggregationGroupByOperator;
import com.linkedin.pinot.core.operator.query.MAggregationOperator;
import com.linkedin.pinot.core.plan.*;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV3;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByOperatorService;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class StarTreeQueriesTest {
  private String testName;
  private File indexDir;
  private File avroFile;
  private IndexSegment indexSegment;
  private int d0Cardinality = 2;
  private int d1Cardinality = 64;
  private int d2Cardinality = 128;
  private int numTimeBuckets = 512;
  private int numRecords = 1024;

  @BeforeClass
  public void beforeClass() throws Exception {
    testName = StarTreeQueriesTest.class.getSimpleName();
    indexDir = new File(System.getProperty("java.io.tmpdir"), testName);
    if (indexDir.exists()) {
      FileUtils.forceDelete(indexDir);
    }
    System.out.println(indexDir);

    avroFile = new File(System.getProperty("java.io.tmpdir"), testName + ".avro");
    if (avroFile.exists()) {
      FileUtils.forceDelete(avroFile);
    }
    avroFile.deleteOnExit();
    createSampleAvroData(avroFile, numRecords, numTimeBuckets);

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(avroFile, indexDir,
            "daysSinceEpoch", TimeUnit.DAYS, "testTable");
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");

    // Set the star tree index config
    StarTreeIndexSpec starTreeIndexSpec = new StarTreeIndexSpec();
    starTreeIndexSpec.setSplitExcludes(Arrays.asList("daysSinceEpoch"));
    starTreeIndexSpec.setMaxLeafRecords(4);
    config.getSchema().setStarTreeIndexSpec(starTreeIndexSpec);

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    File segmentFile = new File(indexDir, driver.getSegmentName());
    indexSegment = ColumnarSegmentLoader.load(segmentFile, ReadMode.heap);
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (System.getProperty("startree.test.keep.dir") == null) {
      FileUtils.forceDelete(indexDir);
    }
  }

  @DataProvider
  public Object[][] filterQueryDataProvider() {
    int numRandom = 1000;
    Random random = new Random();
    List<Object[]> combinations = new ArrayList<>();

    combinations.add(new Object[] {
        ImmutableMap.of("D0", "0", "D1", "0")
    });
    combinations.add(new Object[] {
        ImmutableMap.of("D0", "0")
    });
    combinations.add(new Object[] {
        ImmutableMap.of("D1", "1")
    });
    combinations.add(new Object[] {
        ImmutableMap.of("D2", "1")
    });
    combinations.add(new Object[] {
        ImmutableMap.of()
    });
    combinations.add(new Object[] {
        ImmutableMap.of("D0", "1", "D2", "120")
    });
    combinations.add(new Object[] {
        ImmutableMap.of("D0", "1", "D1", "43", "D2", "43")
    });
    combinations.add(new Object[] {
        ImmutableMap.of("D0", "0", "D1", "40", "D2", "104")
    });

    for (int i = 0; i < numRandom; i++) {
      Map<String, String> combination = new HashMap<>();
      if (random.nextInt(2) == 0) {
        combination.put("D0", String.valueOf(random.nextInt(d0Cardinality)));
      }
      if (random.nextInt(2) == 0) {
        combination.put("D1", String.valueOf(random.nextInt(d1Cardinality)));
      }
      if (random.nextInt(2) == 0) {
        combination.put("D2", String.valueOf(random.nextInt(d2Cardinality)));
      }
      combinations.add(new Object[] {
          combination
      });
    }

    return combinations.toArray(new Object[][] {});
  }

  @DataProvider
  public Object[][] instancePlanMakerDataProvider() {
    return new Object[][] {
        {
            new InstancePlanMakerImplV2()
        }, // test new segment type backward compatibility
        {
            new InstancePlanMakerImplV3()
        },
    };
  }

  @DataProvider
  public Object[][] groupByDataProvider() {
    int numRandom = 1000;
    Random random = new Random();
    List<Object[]> combinations = new ArrayList<>();

    combinations.add(new Object[] {
        ImmutableMap.of("D0", "0"), ImmutableList.of("D1")
    });
    combinations.add(new Object[] {
        ImmutableMap.of("D0", "1"), ImmutableList.of("D2")
    });
    combinations.add(new Object[] {
        ImmutableMap.of("D2", "17"), ImmutableList.of("D0")
    });
    combinations.add(new Object[] {
        ImmutableMap.of("D0", "0"), ImmutableList.of("D1", "D2")
    });

    for (int i = 0; i < numRandom; i++) {
      Map<String, String> combination = new HashMap<>();
      List<String> groupByColumns = new ArrayList<>();
      if (random.nextInt(2) == 0) {
        combination.put("D0", String.valueOf(random.nextInt(d0Cardinality)));
      } else if (random.nextInt(4) == 0) {
        groupByColumns.add("D0");
      }

      if (random.nextInt(2) == 0) {
        combination.put("D1", String.valueOf(random.nextInt(d1Cardinality)));
      } else if (random.nextInt(4) == 0) {
        groupByColumns.add("D1");
      }

      if (random.nextInt(2) == 0) {
        combination.put("D2", String.valueOf(random.nextInt(d2Cardinality)));
      } else if (random.nextInt(4) == 0) {
        groupByColumns.add("D2");
      }

      if (!groupByColumns.isEmpty()) {
        combinations.add(new Object[] {
            combination, groupByColumns
        });
      }
    }

    return combinations.toArray(new Object[][] {});
  }

  @Test(dataProvider = "filterQueryDataProvider", enabled = false)
  public void testPlanMaker(Map<String, String> filterQuery) throws Exception {
    // Build request
    final BrokerRequest brokerRequest = new BrokerRequest();
    final List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getSumAggregationInfo());
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    if (!filterQuery.isEmpty()) {
      String[] flattened = flattenFilterQuery(filterQuery);
      setFilterQuery(brokerRequest, flattened[0], flattened[1]);
    }

    // Compute plan
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV3();
    final PlanNode rootPlanNode =
        instancePlanMaker.makeInnerSegmentPlan(indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    // Assert.assertEquals(rootPlanNode.getClass(), StarTreeAggregationPlanNode.class);

    // Perform aggregation
    final MAggregationOperator operator = (MAggregationOperator) rootPlanNode.run();
    final IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();

    // Get response
    final ReduceService reduceService = new DefaultReduceService();
    final Map<ServerInstance, DataTable> instanceResponseMap =
        new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:1111"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:2222"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:3333"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:4444"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:5555"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:6666"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:7777"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:8888"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:9999"),
        resultBlock.getAggregationResultDataTable());
    final BrokerResponse reducedResults =
        reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);

    // Check (we sent 10 broker requests, so assume 10x)
    Long fromPinot = reducedResults.getAggregationResults().get(0).getLong("value");
    Map<String, Number> fromRawData = computeAggregateFromRawData(avroFile, filterQuery);
    Assert.assertEquals(fromPinot.longValue(),
        fromRawData.get("M0").longValue() * 10 /* because 10 broker requests */);
  }

  @Test(dataProvider = "instancePlanMakerDataProvider", enabled = false)
  public void testRawQuery(PlanMaker instancePlanMaker) throws Exception {
    // Build request
    final BrokerRequest brokerRequest = new BrokerRequest();
    final List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    brokerRequest.setAggregationsInfo(aggregationsInfo);

    // Compute plan
    final PlanNode rootPlanNode =
        instancePlanMaker.makeInnerSegmentPlan(indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    // Assert.assertTrue(rootPlanNode.getClass().equals(RawAggregationPlanNode.class)
    // || rootPlanNode.getClass().equals(AggregationPlanNode.class));

    // Perform aggregation
    final MAggregationOperator operator = (MAggregationOperator) rootPlanNode.run();
    final IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();

    // Get response
    final ReduceService reduceService = new DefaultReduceService();
    final Map<ServerInstance, DataTable> instanceResponseMap =
        new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:1111"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:2222"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:3333"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:4444"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:5555"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:6666"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:7777"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:8888"),
        resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:9999"),
        resultBlock.getAggregationResultDataTable());
    final BrokerResponse reducedResults =
        reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);

    // Check (we sent 10 broker requests, so assume 10x)
    Long fromPinot = reducedResults.getAggregationResults().get(0).getLong("value");
    Assert.assertEquals(fromPinot.intValue(), numRecords * 10 /* because 10 broker requests */);
  }

  @Test(dataProvider = "groupByDataProvider", enabled = false)
  public void testGroupByQuery(Map<String, String> filterQuery, List<String> groupByDimensions)
      throws Exception {
    // Build request
    final BrokerRequest brokerRequest = new BrokerRequest();
    final List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getSumAggregationInfo());
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    if (!filterQuery.isEmpty()) {
      String[] flattened = flattenFilterQuery(filterQuery);
      setFilterQuery(brokerRequest, flattened[0], flattened[1]);
    }
    GroupBy groupBy = new GroupBy();
    groupBy.setColumns(groupByDimensions);
    groupBy.setTopN(1000); // n.b. should be higher than any cardinality for test purposes
    brokerRequest.setGroupBy(groupBy);

    // Compute plan
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV3();
    final PlanNode rootPlanNode =
        instancePlanMaker.makeInnerSegmentPlan(indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    // Assert.assertEquals(rootPlanNode.getClass(),
    // StarTreeAggregationGroupByOperatorPlanNode.class);

    // Perform aggregation
    final MAggregationGroupByOperator operator = (MAggregationGroupByOperator) rootPlanNode.run();
    final IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();

    // Get response
    final AggregationGroupByOperatorService aggregationGroupByOperatorService =
        new AggregationGroupByOperatorService(aggregationsInfo, brokerRequest.getGroupBy());
    final Map<ServerInstance, DataTable> instanceResponseMap =
        new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"),
        resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:1111"),
        resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:2222"),
        resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:3333"),
        resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:4444"),
        resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:5555"),
        resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:6666"),
        resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:7777"),
        resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:8888"),
        resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:9999"),
        resultBlock.getAggregationGroupByResultDataTable());
    final List<Map<String, Serializable>> reducedResults =
        aggregationGroupByOperatorService.reduceGroupByOperators(instanceResponseMap);

    // Check (we sent 10 broker requests, so assume 10x)
    final List<JSONObject> jsonResult =
        aggregationGroupByOperatorService.renderGroupByOperators(reducedResults);

    // Extract the group by sums
    Map<List<String>, Long> groupBySums = new HashMap<>();
    JSONArray groupByResult = jsonResult.get(0).getJSONArray("groupByResult");
    for (int i = 0; i < groupByResult.length(); i++) {
      // Get the group list
      List<String> group = new ArrayList<>();
      JSONArray groupArray = groupByResult.getJSONObject(i).getJSONArray("group");
      for (int j = 0; j < groupArray.length(); j++) {
        group.add(groupArray.getString(j));
      }
      groupBySums.put(group, groupByResult.getJSONObject(i).getLong("value"));
    }

    // Get them from raw data
    Map<List<String>, Long> fromAvro =
        computeAggregateGroupByFromRawData(avroFile, filterQuery, groupByDimensions);

    // Compare
    Assert.assertEquals(groupBySums.size(), fromAvro.size());
    for (List<String> group : fromAvro.keySet()) {
      Long m0Avro = fromAvro.get(group);
      Long m0Pinot = groupBySums.get(group);
      Assert.assertEquals(m0Pinot.longValue(), m0Avro * 10 /* because broker queries */);
    }
  }

  private static String[] flattenFilterQuery(Map<String, String> filterQuery) {
    List<String> keys = new ArrayList<>();
    List<String> vals = new ArrayList<>();
    for (Map.Entry<String, String> entry : filterQuery.entrySet()) {
      keys.add(entry.getKey());
      vals.add(entry.getValue());
    }
    return new String[] {
        Joiner.on(",").join(keys), Joiner.on(",").join(vals)
    };
  }

  private void createSampleAvroData(File file, int numRecords, int numTimeBuckets)
      throws Exception {
    Schema schema = SchemaBuilder.builder().record("TestRecord").fields().name("D0")
        .prop("pinotType", "DIMENSION").type().stringBuilder().endString().noDefault().name("D1")
        .prop("pinotType", "DIMENSION").type().stringBuilder().endString().noDefault().name("D2")
        .prop("pinotType", "DIMENSION").type().stringBuilder().endString().noDefault()
        .name("daysSinceEpoch").prop("pinotType", "TIME").type().longBuilder().endLong().noDefault()
        .name("M0").prop("pinotType", "METRIC").type().longBuilder().endLong().noDefault()
        .name("M1").prop("pinotType", "METRIC").type().doubleBuilder().endDouble().noDefault()
        .endRecord();

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);

    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    fileWriter.create(schema, file);

    for (int i = 0; i < numRecords; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("D0", String.valueOf(i % d0Cardinality));
      record.put("D1", String.valueOf(i % d1Cardinality));
      record.put("D2", String.valueOf(i % d2Cardinality));
      record.put("daysSinceEpoch", (long) (i % numTimeBuckets));
      record.put("M0", 1L);
      record.put("M1", 1.0);
      fileWriter.append(record);
    }

    fileWriter.close();
  }

  private static AggregationInfo getSumAggregationInfo() {
    final String type = "sum";
    final Map<String, String> params = new HashMap<String, String>();
    params.put("column", "M0");
    final AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getCountAggregationInfo() {
    final String type = "count";
    final Map<String, String> params = new HashMap<String, String>();
    params.put("column", "M0");
    final AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static BrokerRequest setFilterQuery(BrokerRequest brokerRequest, String filterColumn,
      String filterVal) {
    FilterQueryTree filterQueryTree;
    if (filterColumn.contains(",")) {
      final String[] filterColumns = filterColumn.split(",");
      final String[] filterValues = filterVal.split(",");
      final List<FilterQueryTree> nested = new ArrayList<FilterQueryTree>();
      for (int i = 0; i < filterColumns.length; i++) {

        final List<String> vals = new ArrayList<String>();
        vals.add(filterValues[i]);
        final FilterQueryTree d =
            new FilterQueryTree(i + 1, filterColumns[i], vals, FilterOperator.EQUALITY, null);
        nested.add(d);
      }
      filterQueryTree = new FilterQueryTree(0, null, null, FilterOperator.AND, nested);
    } else {
      final List<String> vals = new ArrayList<String>();
      vals.add(filterVal);
      filterQueryTree = new FilterQueryTree(0, filterColumn, vals, FilterOperator.EQUALITY, null);
    }
    RequestUtils.generateFilterFromTree(filterQueryTree, brokerRequest);
    return brokerRequest;
  }

  private static Map<String, Number> computeAggregateFromRawData(File avroFile,
      Map<String, String> fixedValues) throws Exception {
    long m0Aggregate = 0;
    double m1Aggregate = 0.0;

    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(avroFile, reader);
    GenericRecord record = null;
    while (fileReader.hasNext()) {
      record = fileReader.next(record);
      boolean matches = true;
      for (Map.Entry<String, String> entry : fixedValues.entrySet()) {
        String value = record.get(entry.getKey()).toString();
        if (!value.equals(entry.getValue())) {
          matches = false;
        }
      }

      if (matches) {
        m0Aggregate += (Long) record.get("M0");
        m1Aggregate += (Double) record.get("M1");
      }
    }

    return ImmutableMap.of("M0", m0Aggregate, "M1", m1Aggregate);
  }

  private static Map<List<String>, Long> computeAggregateGroupByFromRawData(File avroFile,
      Map<String, String> fixedValues, List<String> groupByColumns) throws Exception {
    Map<List<String>, Long> m0Aggregates = new HashMap<>();

    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(avroFile, reader);
    GenericRecord record = null;
    while (fileReader.hasNext()) {
      record = fileReader.next(record);
      boolean matches = true;
      for (Map.Entry<String, String> entry : fixedValues.entrySet()) {
        String value = record.get(entry.getKey()).toString();
        if (!value.equals(entry.getValue())) {
          matches = false;
        }
      }

      if (matches) {
        // Get group
        List<String> group = new ArrayList<>();
        for (String column : groupByColumns) {
          group.add(record.get(column).toString());
        }

        Long sum = m0Aggregates.get(group);
        if (sum == null) {
          sum = 0L;
        }
        m0Aggregates.put(group, sum + (Long) record.get("M0"));
      }
    }

    return m0Aggregates;
  }
}
