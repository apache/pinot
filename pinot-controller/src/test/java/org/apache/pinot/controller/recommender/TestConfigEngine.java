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
package org.apache.pinot.controller.recommender;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.recommender.exceptions.InvalidInputException;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.RulesToExecute;
import org.apache.pinot.controller.recommender.rules.impl.InvertedSortedIndexJointRule;
import org.apache.pinot.controller.recommender.rules.io.configs.SegmentSizeRecommendations;
import org.apache.pinot.controller.recommender.rules.utils.FixedLenBitset;
import org.apache.pinot.controller.recommender.rules.utils.QueryInvertedSortedIndexRecommender;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.recommender.rules.impl.RealtimeProvisioningRule.*;
import static org.testng.Assert.*;


public class TestConfigEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestConfigEngine.class);
  private InputManager _input;

  void loadInput(String fName)
      throws InvalidInputException, IOException {
    _input = JsonUtils.stringToObject(readInputToStr(fName), InputManager.class);
    _input.init();
  }

  private String readInputToStr(String resourceName)
      throws IOException {
    URL resourceUrl = getClass().getClassLoader().getResource(resourceName);
    File file = new File(resourceUrl.getFile());
    return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
  }

  @Test
  void testInputManager()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/SortedInvertedIndexInput.json");
    assertEquals(_input.getSchema().getDimensionNames().toString(), "[a, b, c, d, e, f, g, h, i, j, ja, jb]");
    assertEquals(_input.getOverWrittenConfigs().getIndexConfig().getInvertedIndexColumns().toString(), "[a, b]");
    assertEquals(_input.getBloomFilterRuleParams().getThresholdMinPercentEqBloomfilter().toString(), "0.51");
    assertEquals(_input.getLatencySLA(), 500);
    assertEquals(_input.getColNameToIntMap().size(), 19);
    assertEquals(_input.getFieldType("h"), FieldSpec.DataType.BYTES);
    assertEquals(_input.getFieldType("t"), FieldSpec.DataType.INT);
    assertEquals(_input.getCardinality("g"), 6, 0.00001);
    assertEquals(_input.getCardinality("t"), 10000, 0.00001);
    assertEquals(_input.getNumValuesPerEntry("g"), 2, 0.00001);
    assertEquals(_input.getAverageDataLen("g"), 100);
    assertTrue(_input.isSingleValueColumn("j"));
    assertFalse(_input.isSingleValueColumn("i"));
    assertTrue(_input.getTimeColumns().contains("t"));
  }

  @Test
  void testSupportsBooleanDataType()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/SortedInvertedIndexInput.json");
    assertEquals(_input.getFieldType("ja"), FieldSpec.DataType.BOOLEAN);
    assertEquals(_input.getFieldType("jb"), FieldSpec.DataType.BOOLEAN);
    assertTrue(_input.isSingleValueColumn("ja"));
    assertFalse(_input.isSingleValueColumn("jb"));
    assertEquals(_input.getNumValuesPerEntry("jb"), 3, 0.00001);
  }

  @Test
  void testDataSizeCalculation()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/DataSizeCalculationInput.json");
    assertEquals(_input.getDictionaryEncodedForwardIndexSize("a"), 1);
    assertEquals(_input.getDictionaryEncodedForwardIndexSize("b"), 2);
    assertEquals(_input.getDictionaryEncodedForwardIndexSize("t"), 2);
    assertEquals(_input.getColRawSizePerDoc("a"), 4);
    assertEquals(_input.getColRawSizePerDoc("b"), 8);
    try {
      _input.getColRawSizePerDoc("c");
      Assert.fail("Getting raw size from MV column does not fail");
    } catch (InvalidInputException e) {
      // Expected 409 Conflict
      assertTrue(e.getMessage().startsWith("Column c is MV column should not have raw encoding"));
    }
    assertEquals(_input.getDictionarySize("k"), 65537 * 8);
    assertEquals(_input.getDictionarySize("d"), 1000 * 27 * 2);
    _input.estimateSizePerRecord();
    assertEquals(_input.getSizePerRecord(), 26);
  }

  @Test
  void testInvertedSortedIndexJointRule()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/SortedInvertedIndexInput.json");
    ConfigManager output = new ConfigManager();
    AbstractRule abstractRule =
        RulesToExecute.RuleFactory.getRule(RulesToExecute.Rule.InvertedSortedIndexJointRule, _input, output);
    abstractRule.run();
    assertEquals(output.getIndexConfig().getInvertedIndexColumns().toString(), "[e, f, j]");
    assertEquals(output.getIndexConfig().getSortedColumn(), "c");
  }

  @Test
  void testInvalidColumnInFilterRule()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/InvalidColumnInFilterInput.json");
    ConfigManager output = new ConfigManager();
    AbstractRule abstractRule =
        RulesToExecute.RuleFactory.getRule(RulesToExecute.Rule.InvertedSortedIndexJointRule, _input, output);
    abstractRule.run();
    assertEquals(output.getIndexConfig().getInvertedIndexColumns().toString(), "[]");
    assertEquals(_input.getOverWrittenConfigs().getFlaggedQueries().getFlaggedQueries().toString(),
        "{select i from tableName where a = xyz and t > 500=ERROR: "
            + "Query is filtering on columns not appearing in schema: [xyz]}");
  }

  @Test
  void testSortedInvertedIndexJointRuleWithMetricAndDateTimeColumn()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/SortedInvertedIndexInputWithMetricAndDateTimeColumn.json");
    ConfigManager output = new ConfigManager();
    AbstractRule abstractRule =
        RulesToExecute.RuleFactory.getRule(RulesToExecute.Rule.InvertedSortedIndexJointRule, _input, output);
    abstractRule.run();
    assertEquals(output.getIndexConfig().getInvertedIndexColumns().toString(), "[c, t, x]");
    assertEquals(output.getIndexConfig().getSortedColumn(), "p");
  }

  @Test
  void testEngineEmptyQueries()
      throws InvalidInputException, IOException {
    String input = readInputToStr("recommenderInput/EmptyQueriesInput.json");
    RecommenderDriver.run(input);
  }

  @Test
  void testQueryInvertedSortedIndexRecommender()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/SortedInvertedIndexInput.json");
    QueryInvertedSortedIndexRecommender totalNESICounter =
        QueryInvertedSortedIndexRecommender.QueryInvertedSortedIndexRecommenderBuilder
            .aQueryInvertedSortedIndexRecommender().setInputManager(_input)
            .setInvertedSortedIndexJointRuleParams(_input.getInvertedSortedIndexJointRuleParams())
            .setUseOverwrittenIndices(true) // nESI when not using any overwritten indices
            .build();

    Set<String> results = new HashSet<String>() {{
      add("[[PredicateParseResult{dims{[3]}, AND, BITMAP, nESI=1.645, selected=0.034, nESIWithIdx=0.695}, "
          + "PredicateParseResult{dims{[2]},"
          + " AND, BITMAP, nESI=1.645, selected=0.034, nESIWithIdx=0.835}, PredicateParseResult{dims{[]}, AND, "
          + "NESTED, nESI=1.645, selected=0.034, nESIWithIdx=1.645}]]");
      add("[[PredicateParseResult{dims{[7]}, AND, BITMAP, nESI=0.150, selected=0.015, nESIWithIdx=0.058}, "
          + "PredicateParseResult{dims{[]}, "
          + "AND, NESTED, nESI=0.150, selected=0.015, nESIWithIdx=0.150}], [PredicateParseResult{dims{[5, 9]}, AND, "
          + "BITMAP, nESI=12.000, "
          + "selected=0.500, nESIWithIdx=4.000}, PredicateParseResult{dims{[]}, AND, NESTED, nESI=12.000, selected=0"
          + ".500, nESIWithIdx=12.000}]]");
      add("[[PredicateParseResult{dims{[2, 4]}, AND, BITMAP, nESI=7.625, selected=0.023, nESIWithIdx=1.309}, "
          + "PredicateParseResult{dims{[]}, AND, NESTED, nESI=7.625, selected=0.023, nESIWithIdx=7.625}]]");
    }};

    String q1 =
        "select i from tableName where b in (2,4) and ((a in (1,2,3) and e = 4) or c = 7) and d in ('#VALUES', 23) "
            + "and t > 500";
    String q2 =
        "select j from tableName where (a=3 and (h = 5 or f >34) and REGEXP_LIKE(i, 'as*')) or ((f = 3  or j in "
            + "('#VALUES', 4)) and " + "REGEXP_LIKE(d, 'fl*'))";
    String q3 =
        "select f from tableName where (a=0 or (b=1 and (e in ('#VALUES',2) or c=7))) and TEXT_MATCH(d, 'dasd') and "
            + "MAX(MAX(h,i),j)=4 and" + " t<3";
    assertTrue(results
        .contains(totalNESICounter.parseQuery(_input.getQueryContext(q1), _input.getQueryWeight(q1)).toString()));
    assertTrue(results
        .contains(totalNESICounter.parseQuery(_input.getQueryContext(q2), _input.getQueryWeight(q2)).toString()));
    assertTrue(results
        .contains(totalNESICounter.parseQuery(_input.getQueryContext(q3), _input.getQueryWeight(q3)).toString()));
  }

  @Test(expectedExceptions = InvalidInputException.class)
  void testInvalidInput1()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/InvalidInput1.json");
  }

  @Test(expectedExceptions = InvalidInputException.class)
  void testInvalidInput2()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/InvalidInput2.json");
  }

  @Test
  void testFlagQueryRule()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/FlagQueryInput.json");
    ConfigManager output = _input.getOverWrittenConfigs();
    AbstractRule abstractRule = RulesToExecute.RuleFactory.getRule(RulesToExecute.Rule.FlagQueryRule, _input, output);
    abstractRule.run();

    assertFalse(output.getFlaggedQueries().getFlaggedQueries().containsKey("select f from tableName where x = 2"));
    assertFalse(output.getFlaggedQueries().getFlaggedQueries().containsKey("select f from tableName where t = 3"));
    assertTrue(output.getFlaggedQueries().getFlaggedQueries().containsKey("select * from tableName"));
    assertTrue(output.getFlaggedQueries().getFlaggedQueries().containsKey("select f from tableName"));
    assertTrue(output.getFlaggedQueries().getFlaggedQueries().containsKey("select f from tableName where a =3"));
    assertTrue(output.getFlaggedQueries().getFlaggedQueries().containsKey("select g from tableName LIMIT 1000000000"));
  }

  @Test
  void testVariedLengthDictionaryRule()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/VariedLengthDictionaryInput.json");
    ConfigManager output = _input.getOverWrittenConfigs();
    AbstractRule abstractRule =
        RulesToExecute.RuleFactory.getRule(RulesToExecute.Rule.VariedLengthDictionaryRule, _input, output);
    abstractRule.run();
    assertEquals(output.getIndexConfig().getVarLengthDictionaryColumns().toString(), "[a, d, m]");
  }

  @Test
  void testBloomFilterRule()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/BloomFilterInput.json");
    ConfigManager output = new ConfigManager();
    AbstractRule abstractRule = RulesToExecute.RuleFactory.getRule(RulesToExecute.Rule.BloomFilterRule, _input, output);
    abstractRule.run();
    assertEquals(output.getIndexConfig().getBloomFilterColumns().toString(), "[b, c, e]");
  }

  @Test
  void testBloomFilterRuleWithTimeSpecColumn()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/BloomFilterInputWithDateTimeColumn.json");
    ConfigManager output = new ConfigManager();
    AbstractRule abstractRule = RulesToExecute.RuleFactory.getRule(RulesToExecute.Rule.BloomFilterRule, _input, output);
    abstractRule.run();
    assertEquals(output.getIndexConfig().getBloomFilterColumns().toString(), "[b, t, x]");
  }

  @Test
  void testRangeIndexRule()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/RangeIndexInput.json");
    ConfigManager output = new ConfigManager();
    AbstractRule abstractRule = RulesToExecute.RuleFactory.getRule(RulesToExecute.Rule.RangeIndexRule, _input, output);
    abstractRule.run();
    // Although column i has highest weight, it being string column, range index recommender will skip it and select
    // next winner
    assertNotEquals(output.getIndexConfig().getRangeIndexColumns().toString(), "[i]");
    // index can be supported on dimension, date-time and metric columns
    assertEquals(output.getIndexConfig().getRangeIndexColumns().toString(), "[t, j]");
  }

  /** Verifiy rule that recommends JsonIndex and NoDictionary on JSON columns. */
  @Test
  void testJsonIndexRule()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/SegmentSizeRuleInput.json");
    ConfigManager output = new ConfigManager();
    AbstractRule abstractRule = RulesToExecute.RuleFactory.getRule(RulesToExecute.Rule.JsonIndexRule, _input, output);
    abstractRule.run();
    assertEquals(output.getIndexConfig().getJsonIndexColumns().toString(), "[q]");
    assertEquals(output.getIndexConfig().getNoDictionaryColumns().toString(), "[q]");
  }

  @Test
  void testNoDictionaryOnHeapDictionaryJointRule()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/NoDictionaryOnHeapDictionaryJointRuleInput.json");
    AbstractRule abstractRule = RulesToExecute.RuleFactory
        .getRule(RulesToExecute.Rule.NoDictionaryOnHeapDictionaryJointRule, _input, _input._overWrittenConfigs);
    abstractRule.run();
    assertEquals(_input._overWrittenConfigs.getIndexConfig().getNoDictionaryColumns().toString(),
        "[p, t, h, j, l, m, n, o]");
  }

  @Test
  void testPinotTablePartitionRule()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/PinotTablePartitionRuleInput.json");

    // segment size recommendations get populated by SegmentSize Rule; hard-coding the values here
    _input._overWrittenConfigs.setSegmentSizeRecommendations(
        new SegmentSizeRecommendations(/*numRows=*/1_000_000, /*numSegments=*/4, /*segmentSize=*/1_000_000));

    AbstractRule abstractRule =
        RulesToExecute.RuleFactory.getRule(RulesToExecute.Rule.KafkaPartitionRule, _input, _input._overWrittenConfigs);
    abstractRule.run();
    abstractRule = RulesToExecute.RuleFactory
        .getRule(RulesToExecute.Rule.PinotTablePartitionRule, _input, _input._overWrittenConfigs);
    abstractRule.run();
    ConfigManager output = _input._overWrittenConfigs;
    LOGGER.debug("{} {} {}", output.getPartitionConfig().getPartitionDimension(),
        output.getPartitionConfig().getNumPartitionsRealtime(), output.getPartitionConfig().getNumPartitionsOffline());
    assertEquals(output.getPartitionConfig().getPartitionDimension().toString(), "a");
    assertEquals(output.getPartitionConfig().getNumPartitionsRealtime(), 32);
    assertEquals(output.getPartitionConfig().getNumPartitionsOffline(), 4);
  }

  /**
   * tests PinotTablePartitionRule when multiple metrics and dimensions are available, making sure that weights for
   * dimension fields get set correctly
   */
  @Test
  void testPinotTablePartitionRule2()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/PinotTablePartitionRuleInput2.json");
    AbstractRule abstractRule = RulesToExecute.RuleFactory
        .getRule(RulesToExecute.Rule.PinotTablePartitionRule, _input, _input._overWrittenConfigs);
    abstractRule.run();
    ConfigManager output = _input._overWrittenConfigs;
    LOGGER.debug("{} {} {}", output.getPartitionConfig().getPartitionDimension(),
        output.getPartitionConfig().getNumPartitionsRealtime(), output.getPartitionConfig().getNumPartitionsOffline());
    assertEquals(output.getPartitionConfig().getPartitionDimension().toString(), "colA");
  }

  @Test
  void testKafkaPartitionRule()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/KafkaPartitionRuleInput.json");
    ConfigManager output = new ConfigManager();
    AbstractRule abstractRule =
        RulesToExecute.RuleFactory.getRule(RulesToExecute.Rule.KafkaPartitionRule, _input, output);
    abstractRule.run();
    assertEquals(4, output.getPartitionConfig().getNumKafkaPartitions());
  }

  @Test
  void testKafkaPartitionRule2()
      throws InvalidInputException, IOException {
    loadInput("recommenderInput/KafkaPartitionRuleInput2.json");
    ConfigManager output = new ConfigManager();
    AbstractRule abstractRule =
        RulesToExecute.RuleFactory.getRule(RulesToExecute.Rule.KafkaPartitionRule, _input, output);
    abstractRule.run();
    assertEquals(16, output.getPartitionConfig().getNumKafkaPartitions());
  }

  @Test
  void testPortionSelected() {
    assertEquals(QueryInvertedSortedIndexRecommender.percentSelected(false, 5, 1, 1), 0.2, 0.0000001);
    assertEquals(QueryInvertedSortedIndexRecommender.percentSelected(false, 5, 2, 2), 0.7, 0.0000001);
    assertEquals(QueryInvertedSortedIndexRecommender.percentSelected(true, 8, 2, 4), (double) 15 / 70, 0.0000001);
  }

  @Test
  void testCombinationGenerator() {
    List<int[]> list = InvertedSortedIndexJointRule.generateCombinations(4, 2);
    Set<String> combinations = list.stream().map(l -> Integer.toString(l[0]) + l[1]).collect(Collectors.toSet());

    String[] combinationsInt = {"01", "02", "03", "12", "13", "23"};

    assertEquals(combinations.size(), 6);
    assertTrue(combinations.containsAll(Arrays.asList(combinationsInt)));
  }

  @Test
  void testFixedLenBitset() {
    FixedLenBitset fixedLenBitset = new FixedLenBitset(80);
    Integer[] offsets = {0, 1, 2, 4, 9, 64, 79};
    HashSet<Integer> offsetSet = new HashSet<>(Arrays.asList(offsets));
    for (Integer integer : offsetSet) {
      fixedLenBitset.add(integer);
    }
    fixedLenBitset.add(80);
    for (int i = 0; i < fixedLenBitset.getSize(); i++) {
      if (offsetSet.contains(i)) {
        assertTrue(fixedLenBitset.contains(i));
      } else {
        assertFalse(fixedLenBitset.contains(i));
      }
    }
    assertFalse(fixedLenBitset.contains(80));
    assertEquals(fixedLenBitset.getCardinality(), 7);
    //fixedLenBitset = {0, 1, 2, 4, 9, 64, 79};

    FixedLenBitset fixedLenBitset2 = new FixedLenBitset(80);
    Integer[] offsets2 = {63, 64, 30, 25};
    HashSet<Integer> offsetSet2 = new HashSet<>(Arrays.asList(offsets2));
    for (Integer integer : offsetSet2) {
      fixedLenBitset2.add(integer);
    }
    for (int i = 0; i < fixedLenBitset2.getSize(); i++) {
      if (offsetSet2.contains(i)) {
        assertTrue(fixedLenBitset2.contains(i));
      } else {
        assertFalse(fixedLenBitset2.contains(i));
      }
    }
    fixedLenBitset.union(fixedLenBitset2);
    //fixedLenBitset = {0, 1, 2, 4, 9, 25, 30, 63, 64, 79};
    //fixedLenBitset2 = {63, 64, 30, 25}};

    assertEquals(fixedLenBitset.getCardinality(), 10);
    assertEquals(fixedLenBitset.getOffsets().size(), 10);
    for (int i = 0; i < fixedLenBitset.getSize(); i++) {
      if (offsetSet.contains(i) || offsetSet2.contains(i)) {
        assertTrue(fixedLenBitset.contains(i));
      } else {
        assertFalse(fixedLenBitset.contains(i));
      }
    }

    FixedLenBitset fixedLenBitset3 = new FixedLenBitset(80);
    Integer[] offsets3 = {2, 4, 9, 64, 77};
    HashSet<Integer> offsetSet3 = new HashSet<>(Arrays.asList(offsets3));
    for (Integer integer : offsetSet3) {
      fixedLenBitset3.add(integer);
    }
    //fixedLenBitset = {0, 1, 2, 4, 9, 25, 30, 63, 64, 79};
    //fixedLenBitset2 = {63, 64, 30, 25}};
    //fixedLenBitset3 = {2, 4, 9, 64, 77};

    assertTrue(fixedLenBitset.contains(fixedLenBitset));
    assertTrue(fixedLenBitset.contains(fixedLenBitset2));
    assertFalse(fixedLenBitset.contains(fixedLenBitset3));

    for (int i = 0; i < 80; i++) {
      if (!fixedLenBitset.contains(i) && fixedLenBitset3.contains(i)) {
        assertEquals(i, 77);
      }
    }

    fixedLenBitset.intersect(fixedLenBitset3);
    //fixedLenBitset = {2, 4, 9, 64};

    assertEquals(fixedLenBitset.getCardinality(), 4);
    assertTrue(fixedLenBitset.contains(2));
    assertTrue(fixedLenBitset.contains(4));
    assertTrue(fixedLenBitset.contains(64));
    assertTrue(fixedLenBitset.contains(9));
  }

  @Test
  void testRealtimeProvisioningRuleWithTimeColumn()
      throws Exception {
    testRealtimeProvisioningRule("recommenderInput/RealtimeProvisioningInput_timeColumn.json");
  }

  @Test
  void testRealtimeProvisioningRuleWithDateTimeColumn()
      throws Exception {
    testRealtimeProvisioningRule("recommenderInput/RealtimeProvisioningInput_dateTimeColumn.json");
  }

  @Test
  void testRealtimeProvisioningRuleWithHighIngestionRate()
      throws Exception {
    // Total memory for some of the options are greater than the provided max memory in a host.
    // For those option, the returned values is "NA"
    testRealtimeProvisioningRule("recommenderInput/RealtimeProvisioningInput_highIngestionRate.json");
  }

  @Test
  void testAggregateMetricsRule()
      throws Exception {
    ConfigManager output = runRecommenderDriver("recommenderInput/AggregateMetricsRuleInput.json");
    assertTrue(output.isAggregateMetrics());
  }

  @Test
  void testSegmentSizeRule()
      throws Exception {
    ConfigManager output = runRecommenderDriver("recommenderInput/SegmentSizeRuleInput.json");
    SegmentSizeRecommendations segmentSizeRecommendations = output.getSegmentSizeRecommendations();
    assertEquals(segmentSizeRecommendations.getNumSegments(), 3);
    assertEquals(segmentSizeRecommendations.getNumRowsPerSegment(), 33_333);
  }

  @Test
  void testSegmentSizeRuleNoNeedToGenerateSegment()
      throws Exception {
    ConfigManager output = runRecommenderDriver("recommenderInput/SegmentSizeRuleInput_noNeedToGenerateSegment.json");
    SegmentSizeRecommendations segmentSizeRecommendations = output.getSegmentSizeRecommendations();
    assertEquals(segmentSizeRecommendations.getNumSegments(), 2);
    assertEquals(segmentSizeRecommendations.getNumRowsPerSegment(), 50_000);
  }

  @Test
  void testSegmentSizeRuleRuleIsDisabledButItNeedsToBeSilentlyRun()
      throws Exception {
    ConfigManager output =
        runRecommenderDriver("recommenderInput/SegmentSizeRuleInput_ruleIsDisableButItNeedsToBeSilentlyRun.json");
    assertNull(output.getSegmentSizeRecommendations()); // output is null because the rule silently ran
    assertEquals(output.getPartitionConfig().getPartitionDimension(), "e");
    assertEquals(output.getPartitionConfig().getNumPartitionsOffline(), 2);
  }

  @Test
  void testSegmentSizeRuleRealtimeOnlyTable()
      throws Exception {
    ConfigManager output = runRecommenderDriver("recommenderInput/SegmentSizeRuleInput_realtimeOnlyTable.json");
    assertEquals(output.getSegmentSizeRecommendations().getMessage(),
        "Segment sizing for realtime-only tables is done via Realtime Provisioning Rule");
    assertEquals(output.getSegmentSizeRecommendations().getNumSegments(), 0);
    assertEquals(output.getSegmentSizeRecommendations().getSegmentSize(), 0);
    assertEquals(output.getSegmentSizeRecommendations().getNumRowsPerSegment(), 0);
  }

  private void testRealtimeProvisioningRule(String fileName)
      throws Exception {
    ConfigManager output = runRecommenderDriver(fileName);
    Map<String, Map<String, String>> recommendations = output.getRealtimeProvisioningRecommendations();
    assertRealtimeProvisioningRecommendation(recommendations.get(OPTIMAL_SEGMENT_SIZE));
    assertRealtimeProvisioningRecommendation(recommendations.get(NUM_ROWS_IN_SEGMENT));
    assertRealtimeProvisioningRecommendation(recommendations.get(NUM_SEGMENTS_QUERIED_PER_HOST));
    assertRealtimeProvisioningRecommendation(recommendations.get(CONSUMING_MEMORY_PER_HOST));
    assertRealtimeProvisioningRecommendation(recommendations.get(TOTAL_MEMORY_USED_PER_HOST));
  }

  private ConfigManager runRecommenderDriver(String fileName)
      throws IOException, InvalidInputException {
    String input = readInputToStr(fileName);
    String output = RecommenderDriver.run(input);
    return JsonUtils.stringToObject(output, ConfigManager.class);
  }

  private void assertRealtimeProvisioningRecommendation(Map<String, String> matrix) {
    for (int i = 2; i < 13; i += 2) {
      assertTrue(matrix.containsKey(String.format("numHours - %2d", i)));
    }
    String numHostsRow = matrix.get("numHosts -   ");
    String[] numHosts = numHostsRow.replaceAll("\\s+", ",").split(",");
    assertEquals(numHosts, new String[]{"3", "6", "9", "12", "15", "18", "21"});
  }
}
