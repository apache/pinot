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
package org.apache.pinot.query.runtime.operator;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.planner.plannode.MvAggregateNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.PlanNodeToOpChain;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.BOOLEAN;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.BOOLEAN_ARRAY;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.BYTES;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.BYTES_ARRAY;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.DOUBLE;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.DOUBLE_ARRAY;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.FLOAT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.FLOAT_ARRAY;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT_ARRAY;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.LONG;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.LONG_ARRAY;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING_ARRAY;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.TIMESTAMP;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.TIMESTAMP_ARRAY;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class MvAggregateOperatorTest {
  private AutoCloseable _mocks;
  @Mock
  private MultiStageOperator _input;

  @BeforeMethod
  public void setUp() {
    _mocks = openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @DataProvider(name = "serialized")
  public static Object[][] serialized() {
    return new Object[][]{{false}, {true}};
  }

  /// One group per element, a repeated element counts per copy, an empty array contributes to no group, a null array
  /// forms the null group.
  @Test(dataProvider = "serialized")
  public void testOneMvKeyCountStar(boolean serialized) {
    DataSchema inSchema = new DataSchema(new String[]{"mv"}, new ColumnDataType[]{STRING_ARRAY});
    mockInput(serialized, inSchema, new Object[]{new String[]{"a", "b"}}, new Object[]{new String[]{"b"}},
        new Object[]{new String[0]}, new Object[]{new String[]{"a", "a"}}, new Object[]{null});
    DataSchema resultSchema = new DataSchema(new String[]{"mv", "cnt"}, new ColumnDataType[]{STRING, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(-1), List.of(0), List.of(0));

    Map<List<Object>, List<Object>> result = getResult(operator, 1);

    Map<List<Object>, List<Object>> expected = new HashMap<>();
    expected.put(List.of("a"), List.of(3L));
    expected.put(List.of("b"), List.of(2L));
    expected.put(Arrays.asList((Object) null), List.of(1L));
    assertEquals(result, expected);
    assertTrue(operator.nextBlock().isSuccess(), "Second block is EOS (done processing)");
  }

  /// The SV value of a row is aggregated once per element of its MV key.
  @Test(dataProvider = "serialized")
  public void testMvKeyAndSvKeySum(boolean serialized) {
    DataSchema inSchema =
        new DataSchema(new String[]{"sv", "mv", "v"}, new ColumnDataType[]{INT, STRING_ARRAY, DOUBLE});
    mockInput(serialized, inSchema, new Object[]{1, new String[]{"a", "b"}, 10.0},
        new Object[]{1, new String[]{"a"}, 5.0}, new Object[]{2, new String[]{"a"}, 7.0},
        new Object[]{2, new String[0], 100.0});
    DataSchema resultSchema =
        new DataSchema(new String[]{"sv", "mv", "sum"}, new ColumnDataType[]{INT, STRING, DOUBLE});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getSum(new RexExpression.InputRef(2))), List.of(-1), List.of(0, 1),
            List.of(1));

    Map<List<Object>, List<Object>> result = getResult(operator, 2);

    assertEquals(result,
        Map.of(List.of(1, "a"), List.of(15.0), List.of(1, "b"), List.of(10.0), List.of(2, "a"), List.of(7.0)));
  }

  /// Two MV keys give the cartesian product of their elements.
  @Test(dataProvider = "serialized")
  public void testTwoMvKeysCartesianProduct(boolean serialized) {
    DataSchema inSchema = new DataSchema(new String[]{"ints", "strs"}, new ColumnDataType[]{INT_ARRAY, STRING_ARRAY});
    mockInput(serialized, inSchema, new Object[]{new int[]{1, 2}, new String[]{"x", "y"}});
    DataSchema resultSchema =
        new DataSchema(new String[]{"ints", "strs", "cnt"}, new ColumnDataType[]{INT, STRING, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(-1), List.of(0, 1), List.of(0, 1));

    Map<List<Object>, List<Object>> result = getResult(operator, 2);

    assertEquals(result,
        Map.of(List.of(1, "x"), List.of(1L), List.of(1, "y"), List.of(1L), List.of(2, "x"), List.of(1L),
            List.of(2, "y"), List.of(1L)));
    StatMap<AggregateOperator.StatKey> statMap = getStatMap(operator);
    assertEquals(statMap.getLong(AggregateOperator.StatKey.NUM_GROUPS), 4);
    assertEquals(statMap.getLong(AggregateOperator.StatKey.EMITTED_ROWS), 4);
  }

  /// An SV key between two MV keys keeps its row value in every combination.
  @Test(dataProvider = "serialized")
  public void testSvKeyBetweenTwoMvKeys(boolean serialized) {
    DataSchema inSchema =
        new DataSchema(new String[]{"mv1", "sv", "mv2"}, new ColumnDataType[]{INT_ARRAY, STRING, LONG_ARRAY});
    mockInput(serialized, inSchema, new Object[]{new int[]{1, 2}, "s", new long[]{10L, 20L}},
        new Object[]{new int[]{1}, "t", new long[]{10L, 10L}});
    DataSchema resultSchema =
        new DataSchema(new String[]{"mv1", "sv", "mv2", "cnt"}, new ColumnDataType[]{INT, STRING, LONG, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(-1), List.of(0, 1, 2), List.of(0, 2));

    Map<List<Object>, List<Object>> result = getResult(operator, 3);

    assertEquals(result,
        Map.of(List.of(1, "s", 10L), List.of(1L), List.of(1, "s", 20L), List.of(1L), List.of(2, "s", 10L),
            List.of(1L), List.of(2, "s", 20L), List.of(1L), List.of(1, "t", 10L), List.of(2L)));
  }

  /// For each MV key type: the input array type, the output key type, three arrays over the elements `x` and `y`
  /// (`[x, y]`, `[y]`, `[x, x]`), then `x` and `y` as the output holds them. BOOLEAN and TIMESTAMP use their stored
  /// types, `int` and `long`.
  private static final Object[][] SINGLE_MV_KEY_TYPES = new Object[][]{
      {INT_ARRAY, INT, new int[]{1, 2}, new int[]{2}, new int[]{1, 1}, 1, 2},
      {LONG_ARRAY, LONG, new long[]{1L, 2L}, new long[]{2L}, new long[]{1L, 1L}, 1L, 2L},
      {FLOAT_ARRAY, FLOAT, new float[]{1.5f, 2.5f}, new float[]{2.5f}, new float[]{1.5f, 1.5f}, 1.5f, 2.5f},
      {DOUBLE_ARRAY, DOUBLE, new double[]{1.5, 2.5}, new double[]{2.5}, new double[]{1.5, 1.5}, 1.5, 2.5},
      {BOOLEAN_ARRAY, BOOLEAN, new int[]{1, 0}, new int[]{0}, new int[]{1, 1}, 1, 0},
      {TIMESTAMP_ARRAY, TIMESTAMP, new long[]{1000L, 2000L}, new long[]{2000L}, new long[]{1000L, 1000L}, 1000L,
          2000L},
      {STRING_ARRAY, STRING, new String[]{"x", "y"}, new String[]{"y"}, new String[]{"x", "x"}, "x", "y"}
  };

  @DataProvider(name = "singleMvKeyTypes")
  public static Object[][] singleMvKeyTypes() {
    List<Object[]> cases = new ArrayList<>();
    for (Object[] type : SINGLE_MV_KEY_TYPES) {
      for (boolean serialized : new boolean[]{false, true}) {
        Object[] testCase = Arrays.copyOf(type, type.length + 1);
        testCase[type.length] = serialized;
        cases.add(testCase);
      }
    }
    return cases.toArray(new Object[0][]);
  }

  /// A single MV key of each type uses the one-key group id generator of its stored type: one group per element, a
  /// repeated element counts once per copy.
  @Test(dataProvider = "singleMvKeyTypes")
  public void testSingleMvKeyOfEachType(ColumnDataType inType, ColumnDataType outType, Object row1, Object row2,
      Object row3, Object x, Object y, boolean serialized) {
    DataSchema inSchema = new DataSchema(new String[]{"mv"}, new ColumnDataType[]{inType});
    mockInput(serialized, inSchema, new Object[]{row1}, new Object[]{row2}, new Object[]{row3});
    DataSchema resultSchema = new DataSchema(new String[]{"mv", "cnt"}, new ColumnDataType[]{outType, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(-1), List.of(0), List.of(0));

    Map<List<Object>, List<Object>> result = getResult(operator, 1);

    assertEquals(result, Map.of(List.of(x), List.of(3L), List.of(y), List.of(2L)));
    assertEquals(getStatMap(operator).getLong(AggregateOperator.StatKey.NUM_GROUPS), 2);
  }

  /// A BYTES MV key: `byte[][]` in a row heap block, `ByteArray[]` in a serialized block.
  @Test(dataProvider = "serialized")
  public void testBytesMvKey(boolean serialized) {
    DataSchema inSchema = new DataSchema(new String[]{"mv"}, new ColumnDataType[]{BYTES_ARRAY});
    if (serialized) {
      mockInput(true, inSchema,
          new Object[]{new ByteArray[]{new ByteArray(new byte[]{1}), new ByteArray(new byte[]{2})}},
          new Object[]{new ByteArray[]{new ByteArray(new byte[]{1})}});
    } else {
      mockInput(false, inSchema, new Object[]{new byte[][]{{1}, {2}}}, new Object[]{new byte[][]{{1}}});
    }
    DataSchema resultSchema = new DataSchema(new String[]{"mv", "cnt"}, new ColumnDataType[]{BYTES, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(-1), List.of(0), List.of(0));

    Map<List<Object>, List<Object>> result = getResult(operator, 1);

    assertEquals(result, Map.of(List.of(new ByteArray(new byte[]{1})), List.of(2L),
        List.of(new ByteArray(new byte[]{2})), List.of(1L)));
  }

  /// A LEAF MV aggregate under ORDER BY ... LIMIT keeps the top groups by the intermediate result.
  @Test
  public void testLeafGroupTrim() {
    DataSchema inSchema = new DataSchema(new String[]{"mv"}, new ColumnDataType[]{STRING_ARRAY});
    // Group "g<i>" appears i + 1 times
    Object[][] rows = new Object[7][];
    for (int i = 0; i < 7; i++) {
      String[] values = new String[i + 1];
      Arrays.fill(values, "g" + i);
      rows[i] = new Object[]{values};
    }
    mockInput(false, inSchema, rows);
    DataSchema resultSchema = new DataSchema(new String[]{"mv", "cnt"}, new ColumnDataType[]{STRING, LONG});
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        Map.of(PinotHintOptions.AggregateOptions.MSE_MIN_GROUP_TRIM_SIZE, "1")));
    // Trim size is max(limit * 5, minGroupTrimSize) = 5
    MvAggregateNode node =
        new MvAggregateNode(-1, resultSchema, nodeHint, List.of(), List.of(getCountStar()), List.of(-1), List.of(0),
            AggType.LEAF, false, List.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)), 1,
            List.of(0));
    MvAggregateOperator operator = new MvAggregateOperator(OperatorTestUtil.getContext(Map.of()), _input, node);
    assertEquals(operator.getGroupTrimSize(), 5);

    Map<List<Object>, List<Object>> result = getResult(operator, 1);

    assertEquals(result,
        Map.of(List.of("g6"), List.of(7L), List.of("g5"), List.of(6L), List.of("g4"), List.of(5L), List.of("g3"),
            List.of(4L), List.of("g2"), List.of(3L)));
    StatMap<AggregateOperator.StatKey> statMap = getStatMap(operator);
    assertTrue(statMap.getBoolean(AggregateOperator.StatKey.GROUPS_TRIMMED));
    assertEquals(statMap.getLong(AggregateOperator.StatKey.NUM_GROUPS), 7);
  }

  /// More combinations in one row than an int holds fail the query as a resource limit.
  @Test
  public void testTooManyCombinations() {
    DataSchema inSchema = new DataSchema(new String[]{"a", "b", "c"},
        new ColumnDataType[]{INT_ARRAY, INT_ARRAY, INT_ARRAY});
    int[] values = new int[1300];
    for (int i = 0; i < values.length; i++) {
      values[i] = i;
    }
    // 1300^3 = 2.197e9 combinations
    mockInput(false, inSchema, new Object[]{values, values, values});
    DataSchema resultSchema =
        new DataSchema(new String[]{"a", "b", "c", "cnt"}, new ColumnDataType[]{INT, INT, INT, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(-1), List.of(0, 1, 2), List.of(0, 1, 2));

    MseBlock block = operator.nextBlock();

    assertTrue(block.isError(), "Expected an error block, got: " + block);
    assertEquals(((ErrorMseBlock) block).getMainErrorCode(), QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED);
  }

  /// A filtered aggregation only sees the matched rows, each once per element of its MV key.
  @Test(dataProvider = "serialized")
  public void testFilteredCount(boolean serialized) {
    DataSchema inSchema = new DataSchema(new String[]{"mv", "flag"}, new ColumnDataType[]{STRING_ARRAY, BOOLEAN});
    mockInput(serialized, inSchema, new Object[]{new String[]{"a", "b"}, 1}, new Object[]{new String[]{"a"}, 0},
        new Object[]{new String[]{"b", "b"}, 1}, new Object[]{new String[0], 1}, new Object[]{null, 0});
    DataSchema resultSchema =
        new DataSchema(new String[]{"mv", "cnt", "filteredCnt"}, new ColumnDataType[]{STRING, LONG, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar(), getCountStar()), List.of(-1, 1), List.of(0), List.of(0));

    Map<List<Object>, List<Object>> result = getResult(operator, 1);

    Map<List<Object>, List<Object>> expected = new HashMap<>();
    expected.put(List.of("a"), List.of(2L, 1L));
    expected.put(List.of("b"), List.of(3L, 3L));
    expected.put(Arrays.asList((Object) null), List.of(1L, 0L));
    assertEquals(result, expected);
  }

  /// With only filtered aggregations and empty groups skipped, a group exists only when a matched row forms it.
  @Test(dataProvider = "serialized")
  public void testFilteredCountSkipEmptyGroups(boolean serialized) {
    DataSchema inSchema = new DataSchema(new String[]{"mv", "flag"}, new ColumnDataType[]{STRING_ARRAY, BOOLEAN});
    mockInput(serialized, inSchema, new Object[]{new String[]{"a", "b"}, 1}, new Object[]{new String[]{"c"}, 0});
    DataSchema resultSchema = new DataSchema(new String[]{"mv", "filteredCnt"}, new ColumnDataType[]{STRING, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(1), List.of(0), List.of(0), PlanNode.NodeHint.EMPTY,
            Map.of(QueryOptionKey.FILTERED_AGGREGATIONS_SKIP_EMPTY_GROUPS, "true"));

    Map<List<Object>, List<Object>> result = getResult(operator, 1);

    assertEquals(result, Map.of(List.of("a"), List.of(1L), List.of("b"), List.of(1L)));
  }

  /// Elements past the groups limit are dropped and the stat is set. Without ORDER BY, which groups survive is not
  /// defined, so the test checks only the number of groups and that each surviving group has its full count.
  @Test
  public void testNumGroupsLimit() {
    DataSchema inSchema = new DataSchema(new String[]{"mv"}, new ColumnDataType[]{STRING_ARRAY});
    mockInput(false, inSchema, new Object[]{new String[]{"a", "b", "c"}}, new Object[]{new String[]{"c", "a"}});
    DataSchema resultSchema = new DataSchema(new String[]{"mv", "cnt"}, new ColumnDataType[]{STRING, LONG});
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        Map.of(PinotHintOptions.AggregateOptions.NUM_GROUPS_LIMIT, "2")));
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(-1), List.of(0), List.of(0), nodeHint, Map.of());

    Map<List<Object>, List<Object>> result = getResult(operator, 1);

    assertEquals(result.size(), 2, result.toString());
    Map<List<Object>, List<Object>> fullCounts =
        Map.of(List.of("a"), List.of(2L), List.of("b"), List.of(1L), List.of("c"), List.of(2L));
    for (Map.Entry<List<Object>, List<Object>> entry : result.entrySet()) {
      assertEquals(entry.getValue(), fullCounts.get(entry.getKey()), result.toString());
    }
    StatMap<AggregateOperator.StatKey> statMap = getStatMap(operator);
    assertTrue(statMap.getBoolean(AggregateOperator.StatKey.NUM_GROUPS_LIMIT_REACHED));
    assertEquals(statMap.getLong(AggregateOperator.StatKey.NUM_GROUPS), 2);
  }

  /// The stats are reported under the MV aggregate type, with the aggregate stat keys.
  @Test
  public void testStatsType() {
    DataSchema inSchema = new DataSchema(new String[]{"mv"}, new ColumnDataType[]{STRING_ARRAY});
    mockInput(false, inSchema, new Object[]{new String[]{"a", "b"}});
    DataSchema resultSchema = new DataSchema(new String[]{"mv", "cnt"}, new ColumnDataType[]{STRING, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(-1), List.of(0), List.of(0));

    getResult(operator, 1);
    assertTrue(operator.nextBlock().isSuccess());

    MultiStageQueryStats.StageStats stageStats = operator.calculateStats().getCurrentStats();
    assertSame(stageStats.getOperatorType(stageStats.getLastOperatorIndex()), MultiStageOperator.Type.MV_AGGREGATE);
    StatMap<AggregateOperator.StatKey> statMap = getStatMap(operator);
    assertEquals(statMap.getLong(AggregateOperator.StatKey.EMITTED_ROWS), 2);
    assertEquals(statMap.getLong(AggregateOperator.StatKey.NUM_GROUPS), 2);
    assertFalse(statMap.getBoolean(AggregateOperator.StatKey.NUM_GROUPS_LIMIT_REACHED));
  }

  /// Scalar functions can produce a `List` (for example a `LongArrayList`) or a primitive array.
  @Test
  public void testListAndPrimitiveArrayValues() {
    DataSchema inSchema = new DataSchema(new String[]{"longs", "doubles"}, new ColumnDataType[]{LONG_ARRAY,
        DOUBLE_ARRAY});
    mockInput(false, inSchema, new Object[]{new LongArrayList(new long[]{1L, 2L}), new double[]{0.5}},
        new Object[]{List.of(2L), new double[]{0.5, 1.5}});
    DataSchema resultSchema =
        new DataSchema(new String[]{"longs", "doubles", "cnt"}, new ColumnDataType[]{LONG, DOUBLE, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(-1), List.of(0, 1), List.of(0, 1));

    Map<List<Object>, List<Object>> result = getResult(operator, 2);

    assertEquals(result,
        Map.of(List.of(1L, 0.5), List.of(1L), List.of(2L, 0.5), List.of(2L), List.of(2L, 1.5), List.of(1L)));
  }

  @Test
  public void testUpstreamErrorPropagates() {
    when(_input.nextBlock()).thenReturn(ErrorMseBlock.fromException(new Exception("foo!")));
    DataSchema resultSchema = new DataSchema(new String[]{"mv", "cnt"}, new ColumnDataType[]{STRING, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(-1), List.of(0), List.of(0));

    assertTrue(operator.nextBlock().isError(), "Input errors should propagate immediately");
  }

  @Test
  public void testEmptyInput() {
    when(_input.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    DataSchema resultSchema = new DataSchema(new String[]{"mv", "cnt"}, new ColumnDataType[]{STRING, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(-1), List.of(0), List.of(0));

    assertTrue(operator.nextBlock().isSuccess(), "EOS blocks should propagate");
  }

  /// The MV keys only change raw input, so the operator rejects intermediate input.
  @Test
  public void testRejectsIntermediateInput() {
    DataSchema resultSchema = new DataSchema(new String[]{"mv", "cnt"}, new ColumnDataType[]{STRING, LONG});
    MvAggregateNode node =
        new MvAggregateNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), List.of(getCountStar()),
            List.of(-1), List.of(0), AggType.FINAL, false, null, 0, List.of(0));
    assertThrows(IllegalStateException.class,
        () -> new MvAggregateOperator(OperatorTestUtil.getContext(Map.of()), _input, node));
  }

  @Test
  public void testExplainAttributes() {
    DataSchema resultSchema =
        new DataSchema(new String[]{"sv", "mv", "cnt"}, new ColumnDataType[]{INT, STRING, LONG});
    MvAggregateOperator operator =
        getOperator(resultSchema, List.of(getCountStar()), List.of(-1), List.of(0, 1), List.of(1));

    Map<String, Plan.ExplainNode.AttributeValue> attributes = operator.getExplainInfo().getAttributes();
    assertEquals(attributes.get("mvKeys").getStringList().getValuesList(), List.of("1"));
  }

  /// The op chain builder turns an `MvAggregateNode` into an [MvAggregateOperator].
  @Test
  public void testPlanNodeToOpChain() {
    DataSchema inSchema = new DataSchema(new String[]{"mv"}, new ColumnDataType[]{STRING_ARRAY});
    ValueNode valueNode = new ValueNode(0, inSchema, PlanNode.NodeHint.EMPTY, List.of(), List.of());
    DataSchema resultSchema = new DataSchema(new String[]{"mv", "cnt"}, new ColumnDataType[]{STRING, LONG});
    MvAggregateNode node =
        new MvAggregateNode(0, resultSchema, PlanNode.NodeHint.EMPTY, List.of(valueNode), List.of(getCountStar()),
            List.of(-1), List.of(0), AggType.DIRECT, false, null, 0, List.of(0));

    MultiStageOperator root = PlanNodeToOpChain.convert(node, OperatorTestUtil.getContext(Map.of())).getRoot();

    assertTrue(root instanceof MvAggregateOperator, "Unexpected operator: " + root);
  }

  private void mockInput(boolean serialized, DataSchema schema, Object[]... rows) {
    MseBlock block;
    if (serialized) {
      try {
        block = new SerializedDataBlock(DataBlockBuilder.buildFromRows(Arrays.asList(rows), schema));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      block = OperatorTestUtil.block(schema, rows);
    }
    when(_input.nextBlock()).thenReturn(block).thenReturn(SuccessMseBlock.INSTANCE);
    when(_input.calculateStats()).thenReturn(MultiStageQueryStats.emptyStats(0));
  }

  /// Returns the output rows keyed by their group keys.
  private static Map<List<Object>, List<Object>> getResult(MvAggregateOperator operator, int numKeys) {
    MseBlock block = operator.nextBlock();
    assertTrue(block.isData(), "Expected a data block, got: " + block);
    Map<List<Object>, List<Object>> result = new HashMap<>();
    for (Object[] row : ((MseBlock.Data) block).asRowHeap().getRows()) {
      List<Object> values = Arrays.asList(row);
      List<Object> previous =
          result.put(new ArrayList<>(values.subList(0, numKeys)), new ArrayList<>(values.subList(numKeys,
              row.length)));
      assertEquals(previous, null, "Duplicate group: " + values);
    }
    return result;
  }

  private static StatMap<AggregateOperator.StatKey> getStatMap(MvAggregateOperator operator) {
    StatMap<AggregateOperator.StatKey> statMap =
        OperatorTestUtil.getStatMap(AggregateOperator.StatKey.class, operator.calculateStats());
    assertNotNull(statMap);
    return statMap;
  }

  private static RexExpression.FunctionCall getCountStar() {
    return new RexExpression.FunctionCall(ColumnDataType.LONG, SqlKind.COUNT.name(), List.of());
  }

  private static RexExpression.FunctionCall getSum(RexExpression arg) {
    return new RexExpression.FunctionCall(ColumnDataType.DOUBLE, SqlKind.SUM.name(), List.of(arg));
  }

  private MvAggregateOperator getOperator(DataSchema resultSchema, List<RexExpression.FunctionCall> aggCalls,
      List<Integer> filterArgs, List<Integer> groupKeys, List<Integer> mvKeyIndexes) {
    return getOperator(resultSchema, aggCalls, filterArgs, groupKeys, mvKeyIndexes, PlanNode.NodeHint.EMPTY,
        Map.of());
  }

  private MvAggregateOperator getOperator(DataSchema resultSchema, List<RexExpression.FunctionCall> aggCalls,
      List<Integer> filterArgs, List<Integer> groupKeys, List<Integer> mvKeyIndexes, PlanNode.NodeHint nodeHint,
      Map<String, String> opChainMetadata) {
    return new MvAggregateOperator(OperatorTestUtil.getContext(opChainMetadata), _input,
        new MvAggregateNode(-1, resultSchema, nodeHint, List.of(), aggCalls, filterArgs, groupKeys, AggType.DIRECT,
            false, null, 0, mvKeyIndexes));
  }
}
