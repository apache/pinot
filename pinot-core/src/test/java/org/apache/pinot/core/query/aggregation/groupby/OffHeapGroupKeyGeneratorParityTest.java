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
package org.apache.pinot.core.query.aggregation.groupby;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/// Generator-level differential test: for every group key generator variant, the on-heap and off-heap instances
/// are driven over the same projection blocks and must emit identical group id arrays, identical
/// (groupId -> keys) mappings, and matching counts.
///
/// Each generator gets its own fresh [BaseProjectOperator] over the same immutable segment: the map-based
/// single-MV-column path overwrites the block's cached dictionary-id arrays in place (in both modes), so two
/// generators must not share one operator's block cache. The blocks produced by the two operators are identical
/// because the segment and query are.
///
/// Null-group counting: both modes count the null group in `getNumKeys()` / `getCurrentGroupKeyUpperBound()` —
/// the on-heap map-size-based counting used to exclude it for primitive stored types (INT/LONG/FLOAT/DOUBLE,
/// whose null group lives outside the primitive map), which under-sized result holders; that was fixed alongside
/// the off-heap work, so the counts must now match exactly in every mode and for every stored type.
///
/// Dictionary-based holder selection (arrayBasedThreshold = 10_000, default numGroupsLimit = 100_000), against
/// dict columns `s1..s10` of cardinality 100 and MV columns `m1`/`m2` of cardinality 100:
/// - `s1` -> 100 -> ARRAY_BASED (on-heap in both modes by design)
/// - `s1,s2,s3` -> 10^6 -> INT_MAP_BASED
/// - `s1..s5` -> 10^10 > Integer.MAX_VALUE -> LONG_MAP_BASED
/// - `s1..s10` -> 10^20 > Long.MAX_VALUE -> ARRAY_MAP_BASED
/// - `mHigh` (cardinality ~14_500) -> INT_MAP_BASED for a single MV column (the in-place group id path)
///
/// Every generator is closed after its run, and the test asserts that
/// [PinotDataBuffer#getDirectBufferUsage()] returns to the pre-test baseline after every comparison and at class
/// end (segments are mmap-loaded, so they do not count as direct memory).
public class OffHeapGroupKeyGeneratorParityTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "OffHeapGroupKeyGeneratorParityTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final long RANDOM_SEED = 13;
  private static final int NUM_RECORDS = 25_000;
  private static final int NUM_GROUPS_LIMIT = Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_LIMIT;
  private static final int ARRAY_BASED_THRESHOLD = Server.DEFAULT_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY;
  private static final int MAX_DOCS_PER_BLOCK = DocIdSetPlanNode.MAX_DOC_PER_CALL;

  // Dict-encoded INT SV columns of cardinality 100 each
  private static final String[] DICT_SV_COLUMNS = {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"};
  // Dict-encoded INT MV columns of cardinality 100, plus a high-cardinality MV column for the IntMap MV path
  private static final String M1 = "m1";
  private static final String M2 = "m2";
  private static final String M_HIGH = "mHigh";
  // Raw (no-dictionary) SV columns
  private static final String R_INT = "rInt";
  private static final String R_LONG = "rLong";
  private static final String R_FLOAT = "rFloat";
  private static final String R_DOUBLE = "rDouble";
  private static final String R_STRING = "rString";
  private static final String R_BYTES = "rBytes";
  private static final String R_BIG_DECIMAL = "rBigDecimal";
  private static final String[] RAW_COLUMNS = {R_INT, R_LONG, R_FLOAT, R_DOUBLE, R_STRING, R_BYTES, R_BIG_DECIMAL};
  // Nullable raw columns. "nf" columns have the FIRST row null (plus more sprinkled); "nm" columns have values
  // until row 12_000, nulls for rows [12_000, 13_000), then the full pool so new values first appear after the
  // null stretch (exercising the off-heap null shift)
  private static final String NF_INT = "nfInt";
  private static final String NM_INT = "nmInt";
  private static final String NF_DOUBLE = "nfDouble";
  private static final String NF_STR = "nfStr";
  private static final String NM_STR = "nmStr";
  private static final String NM_BIG_DECIMAL = "nmBigDecimal";
  private static final String[] NULLABLE_COLUMNS = {NF_INT, NM_INT, NF_DOUBLE, NF_STR, NM_STR, NM_BIG_DECIMAL};
  // "nm" columns: 8 distinct values (ids 0-7) appear before the nulls, so the null group takes dense id 8
  private static final int NULL_MID_GROUP_ID = 8;

  private IndexSegment _indexSegment;
  private QueryContext _queryContext;
  private ExpressionContext[] _projectionExpressions;
  private long _directBufferBaseline;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    List<String> allColumns = new ArrayList<>(Arrays.asList(DICT_SV_COLUMNS));
    allColumns.addAll(Arrays.asList(M1, M2, M_HIGH));
    allColumns.addAll(Arrays.asList(RAW_COLUMNS));
    allColumns.addAll(Arrays.asList(NULLABLE_COLUMNS));

    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME);
    for (String column : DICT_SV_COLUMNS) {
      schemaBuilder.addSingleValueDimension(column, DataType.INT);
    }
    schemaBuilder.addMultiValueDimension(M1, DataType.INT);
    schemaBuilder.addMultiValueDimension(M2, DataType.INT);
    schemaBuilder.addMultiValueDimension(M_HIGH, DataType.INT);
    schemaBuilder.addSingleValueDimension(R_INT, DataType.INT);
    schemaBuilder.addSingleValueDimension(R_LONG, DataType.LONG);
    schemaBuilder.addSingleValueDimension(R_FLOAT, DataType.FLOAT);
    schemaBuilder.addSingleValueDimension(R_DOUBLE, DataType.DOUBLE);
    schemaBuilder.addSingleValueDimension(R_STRING, DataType.STRING);
    schemaBuilder.addSingleValueDimension(R_BYTES, DataType.BYTES);
    schemaBuilder.addSingleValueDimension(R_BIG_DECIMAL, DataType.BIG_DECIMAL);
    schemaBuilder.addSingleValueDimension(NF_INT, DataType.INT);
    schemaBuilder.addSingleValueDimension(NM_INT, DataType.INT);
    schemaBuilder.addSingleValueDimension(NF_DOUBLE, DataType.DOUBLE);
    schemaBuilder.addSingleValueDimension(NF_STR, DataType.STRING);
    schemaBuilder.addSingleValueDimension(NM_STR, DataType.STRING);
    schemaBuilder.addSingleValueDimension(NM_BIG_DECIMAL, DataType.BIG_DECIMAL);
    Schema schema = schemaBuilder.build();

    List<String> noDictionaryColumns = new ArrayList<>(Arrays.asList(RAW_COLUMNS));
    noDictionaryColumns.addAll(Arrays.asList(NULLABLE_COLUMNS));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(noDictionaryColumns).build();

    Random random = new Random(RANDOM_SEED);
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      for (int c = 0; c < DICT_SV_COLUMNS.length; c++) {
        record.putValue(DICT_SV_COLUMNS[c], c * 1_000_000 + random.nextInt(100));
      }
      record.putValue(M1, randomMvValues(random, 100, 20_000_000));
      record.putValue(M2, randomMvValues(random, 100, 21_000_000));
      record.putValue(M_HIGH, randomMvValues(random, 15_000, 22_000_000));
      int rawPoolIndex = random.nextInt(150);
      record.putValue(R_INT, rawPoolIndex * 3 - 200);
      record.putValue(R_LONG, rawPoolIndex * 1_000_003L);
      record.putValue(R_FLOAT, floatPoolValue(rawPoolIndex));
      record.putValue(R_DOUBLE, doublePoolValue(rawPoolIndex));
      record.putValue(R_STRING, rawPoolIndex < 5 ? "s😀" + rawPoolIndex : "str_" + rawPoolIndex);
      record.putValue(R_BYTES, new byte[]{
          (byte) rawPoolIndex, (byte) (rawPoolIndex >> 4), (byte) (rawPoolIndex * 3), 42, (byte) i
      });
      record.putValue(R_BIG_DECIMAL, BigDecimal.valueOf((rawPoolIndex - 75) * 25L, 2));
      boolean nullFirstIsNull = i % 7 == 0;
      int nullFirstPoolIndex = i % 9;
      boolean nullMidIsNull = i >= 12_000 && i < 13_000;
      int nullMidPoolIndex = i < 12_000 ? i % 8 : i % 11;
      record.putValue(NF_INT, nullFirstIsNull ? null : nullFirstPoolIndex * 3 - 15);
      record.putValue(NM_INT, nullMidIsNull ? null : nullMidPoolIndex * 3 - 15);
      record.putValue(NF_DOUBLE, nullFirstIsNull ? null : (nullFirstPoolIndex - 5) * 0.5d);
      record.putValue(NF_STR, nullFirstIsNull ? null : "ns_" + nullFirstPoolIndex);
      record.putValue(NM_STR, nullMidIsNull ? null : "ns_" + nullMidPoolIndex);
      record.putValue(NM_BIG_DECIMAL, nullMidIsNull ? null : BigDecimal.valueOf((nullMidPoolIndex - 5) * 25L, 2));
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setDefaultNullHandlingEnabled(true);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();
    _indexSegment = ImmutableSegmentLoader.load(new File(TEMP_DIR, SEGMENT_NAME), ReadMode.mmap);

    _queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable GROUP BY " + StringUtils.join(allColumns, ", "));
    _projectionExpressions = getExpressions(allColumns.toArray(new String[0]));
    // Bytes-key group-id runs allocate the same warm-up shapes each time; capture the baseline before any
    // generator is created
    _directBufferBaseline = PinotDataBuffer.getDirectBufferUsage();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), _directBufferBaseline,
        "Off-heap direct memory leaked by group key generators");
    _indexSegment.destroy();
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  private static Object[] randomMvValues(Random random, int cardinality, int base) {
    int numValues = 1 + random.nextInt(3);
    Object[] values = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = base + random.nextInt(cardinality);
    }
    return values;
  }

  private static float floatPoolValue(int poolIndex) {
    if (poolIndex == 0) {
      return -0.0f;
    }
    if (poolIndex == 1) {
      return 0.0f;
    }
    return (poolIndex - 75) * 0.25f;
  }

  private static double doublePoolValue(int poolIndex) {
    if (poolIndex == 0) {
      return -0.0d;
    }
    if (poolIndex == 1) {
      return 0.0d;
    }
    return (poolIndex - 75) * 0.5d;
  }

  private static ExpressionContext[] getExpressions(String[] columns) {
    ExpressionContext[] expressions = new ExpressionContext[columns.length];
    for (int i = 0; i < columns.length; i++) {
      expressions[i] = ExpressionContext.forIdentifier(columns[i]);
    }
    return expressions;
  }

  private BaseProjectOperator<?> createProjectOperator() {
    return new ProjectPlanNode(new SegmentContext(_indexSegment), _queryContext,
        Arrays.asList(_projectionExpressions), MAX_DOCS_PER_BLOCK).run();
  }

  // ---------------------------------------------------------------------------------------------
  // Run harness
  // ---------------------------------------------------------------------------------------------

  private interface GeneratorFactory {
    GroupKeyGenerator create(BaseProjectOperator<?> projectOperator);
  }

  private static class RunResult {
    // Single-value group ids per block (null for MV runs)
    final List<int[]> _svBlockGroupIds = new ArrayList<>();
    // Multi-value group ids per block (null for SV runs)
    final List<int[][]> _mvBlockGroupIds = new ArrayList<>();
    final List<Integer> _numKeysPerBlock = new ArrayList<>();
    final List<Integer> _upperBoundPerBlock = new ArrayList<>();
    Map<Integer, List<Object>> _groupKeys;
    int _numKeys;
    int _upperBound;
    int _globalUpperBound;
    // Highest direct-buffer usage observed while the generator was open (guards against the off-heap flag being
    // silently ignored, which would make every parity assertion pass vacuously)
    long _peakDirectUsage;
  }

  private RunResult run(GeneratorFactory factory, boolean multiValue) {
    RunResult result = new RunResult();
    BaseProjectOperator<?> projectOperator = createProjectOperator();
    GroupKeyGenerator generator = factory.create(projectOperator);
    try {
      result._peakDirectUsage = PinotDataBuffer.getDirectBufferUsage();
      result._globalUpperBound = generator.getGlobalGroupKeyUpperBound();
      int[] svBuffer = multiValue ? null : new int[MAX_DOCS_PER_BLOCK];
      int[][] mvBuffer = multiValue ? new int[MAX_DOCS_PER_BLOCK][] : null;
      ValueBlock block;
      while ((block = projectOperator.nextBlock()) != null) {
        int numDocs = block.getNumDocs();
        if (multiValue) {
          generator.generateKeysForBlock(block, mvBuffer);
          int[][] blockGroupIds = new int[numDocs][];
          for (int i = 0; i < numDocs; i++) {
            // Deep-copy: the buffer rows may alias (and the map-based single-MV path mutates) block cache arrays
            blockGroupIds[i] = mvBuffer[i].clone();
          }
          result._mvBlockGroupIds.add(blockGroupIds);
        } else {
          generator.generateKeysForBlock(block, svBuffer);
          result._svBlockGroupIds.add(Arrays.copyOf(svBuffer, numDocs));
        }
        result._numKeysPerBlock.add(generator.getNumKeys());
        result._upperBoundPerBlock.add(generator.getCurrentGroupKeyUpperBound());
        result._peakDirectUsage = Math.max(result._peakDirectUsage, PinotDataBuffer.getDirectBufferUsage());
      }
      result._groupKeys = collectGroupKeys(generator);
      result._numKeys = generator.getNumKeys();
      result._upperBound = generator.getCurrentGroupKeyUpperBound();
    } finally {
      generator.close();
    }
    return result;
  }

  private static Map<Integer, List<Object>> collectGroupKeys(GroupKeyGenerator generator) {
    Map<Integer, List<Object>> groupKeys = new HashMap<>();
    Iterator<GroupKeyGenerator.GroupKey> iterator = generator.getGroupKeys();
    while (iterator.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = iterator.next();
      // The GroupKey is a reused flyweight; copy the keys out
      assertNull(groupKeys.put(groupKey._groupId, Arrays.asList(groupKey._keys.clone())),
          "Iterator yielded duplicate group id: " + groupKey._groupId);
    }
    return groupKeys;
  }

  // ---------------------------------------------------------------------------------------------
  // Comparison helpers
  // ---------------------------------------------------------------------------------------------

  /// Compares an on-heap run against an off-heap run.
  ///
  /// @param primitiveNullDivergence historical name: marks runs over a single no-dict column of a primitive
  ///        stored type with null handling enabled. Since the on-heap null-group counting fix landed alongside
  ///        the off-heap work, both modes count the null group and every count must match exactly — the flag is
  ///        retained only to document which runs carry a null group outside the map.
  /// @param denseIds whether the generator assigns dense ids `0..numKeys-1` (all map-based variants; false only
  ///        for the dict ARRAY_BASED holder, whose ids are raw cardinality-products).
  private void compareRuns(RunResult onHeap, RunResult offHeap, boolean primitiveNullDivergence, boolean denseIds,
      String context) {
    assertEquals(offHeap._globalUpperBound, onHeap._globalUpperBound, context + ": globalGroupKeyUpperBound");
    boolean multiValue = !onHeap._mvBlockGroupIds.isEmpty() || !offHeap._mvBlockGroupIds.isEmpty();
    int numBlocks = onHeap._numKeysPerBlock.size();
    assertEquals(offHeap._numKeysPerBlock.size(), numBlocks, context + ": block count");

    // Group ids emitted per block must be identical arrays, and the per-block counts must match exactly (both
    // modes count the primitive-type null group since the on-heap counting fix)
    for (int b = 0; b < numBlocks; b++) {
      if (multiValue) {
        int[][] onHeapIds = onHeap._mvBlockGroupIds.get(b);
        int[][] offHeapIds = offHeap._mvBlockGroupIds.get(b);
        assertEquals(offHeapIds.length, onHeapIds.length, context + ": numDocs in block " + b);
        for (int i = 0; i < onHeapIds.length; i++) {
          assertTrue(Arrays.equals(offHeapIds[i], onHeapIds[i]),
              context + ": MV group ids differ in block " + b + " at doc " + i + ": expected "
                  + Arrays.toString(onHeapIds[i]) + " but got " + Arrays.toString(offHeapIds[i]));
        }
      } else {
        int[] onHeapIds = onHeap._svBlockGroupIds.get(b);
        int[] offHeapIds = offHeap._svBlockGroupIds.get(b);
        assertTrue(Arrays.equals(offHeapIds, onHeapIds),
            context + ": SV group ids differ in block " + b + " at doc " + firstMismatch(onHeapIds, offHeapIds));
      }
      assertEquals(offHeap._numKeysPerBlock.get(b), onHeap._numKeysPerBlock.get(b),
          context + ": getNumKeys after block " + b);
      assertEquals(offHeap._upperBoundPerBlock.get(b), onHeap._upperBoundPerBlock.get(b),
          context + ": getCurrentGroupKeyUpperBound after block " + b);
    }
    assertEquals(offHeap._numKeys, onHeap._numKeys, context + ": final getNumKeys");
    assertEquals(offHeap._upperBound, onHeap._upperBound, context + ": final getCurrentGroupKeyUpperBound");

    // The iterators must yield the same (groupId -> keys) mapping (iterator order may differ)
    assertEquals(offHeap._groupKeys, onHeap._groupKeys, context + ": group key mapping");

    if (denseIds) {
      assertDenseIds(offHeap, context + " (off-heap)");
      // On-heap map-based variants are dense too; verifying both pins the shared contract
      assertDenseIds(onHeap, context + " (on-heap)");
      // The dense (map-based) variants are exactly the ones that must go off-heap: assert the off-heap run
      // actually held direct memory while open, so a silently ignored offHeap flag cannot pass this test
      assertTrue(offHeap._peakDirectUsage > _directBufferBaseline,
          context + ": off-heap generator never allocated direct memory");
    } else {
      // The dict ARRAY_BASED (T0) holder stays on-heap by design even in off-heap mode
      assertEquals(offHeap._peakDirectUsage, _directBufferBaseline,
          context + ": ARRAY_BASED holder unexpectedly allocated direct memory");
    }
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), _directBufferBaseline,
        context + ": off-heap direct memory leaked");
  }

  private static int firstMismatch(int[] expected, int[] actual) {
    for (int i = 0; i < Math.min(expected.length, actual.length); i++) {
      if (expected[i] != actual[i]) {
        return i;
      }
    }
    return -1;
  }

  /// Returns the group id mapped to a single null key, or null if no null group exists.
  private static Integer findNullGroupId(Map<Integer, List<Object>> groupKeys) {
    Integer nullGroupId = null;
    for (Map.Entry<Integer, List<Object>> entry : groupKeys.entrySet()) {
      List<Object> keys = entry.getValue();
      if (keys.size() == 1 && keys.get(0) == null) {
        assertNull(nullGroupId, "Multiple null groups found: " + nullGroupId + " and " + entry.getKey());
        nullGroupId = entry.getKey();
      }
    }
    return nullGroupId;
  }

  /// Asserts the iterator emitted dense ids `0..numKeys-1` with no gaps or duplicates.
  private static void assertDenseIds(RunResult result, String context) {
    TreeSet<Integer> ids = new TreeSet<>(result._groupKeys.keySet());
    assertEquals(ids.size(), result._groupKeys.size(), context + ": duplicate ids");
    if (!ids.isEmpty()) {
      assertEquals((int) ids.first(), 0, context + ": ids must start at 0");
      assertEquals((int) ids.last(), ids.size() - 1, context + ": ids must be dense (no gaps)");
    }
  }

  // ---------------------------------------------------------------------------------------------
  // DictionaryBasedGroupKeyGenerator
  // ---------------------------------------------------------------------------------------------

  private void compareDictionary(String[] columns, int numGroupsLimit, boolean multiValue, boolean denseIds) {
    String context = "Dictionary" + (multiValue ? " MV " : " SV ") + Arrays.toString(columns) + " limit "
        + numGroupsLimit;
    RunResult onHeap = run(op -> new DictionaryBasedGroupKeyGenerator(op, getExpressions(columns), numGroupsLimit,
        ARRAY_BASED_THRESHOLD, false, null, false), multiValue);
    RunResult offHeap = run(op -> new DictionaryBasedGroupKeyGenerator(op, getExpressions(columns), numGroupsLimit,
        ARRAY_BASED_THRESHOLD, false, null, true), multiValue);
    compareRuns(onHeap, offHeap, false, denseIds, context);
  }

  @Test
  public void testDictionarySingleValueVariants() {
    // ARRAY_BASED (product 100): the T0 path stays on-heap in both modes by design
    compareDictionary(new String[]{"s1"}, NUM_GROUPS_LIMIT, false, false);
    // INT_MAP_BASED (product 10^6)
    compareDictionary(new String[]{"s1", "s2", "s3"}, NUM_GROUPS_LIMIT, false, true);
    // LONG_MAP_BASED (product 10^10 > Integer.MAX_VALUE)
    compareDictionary(new String[]{"s1", "s2", "s3", "s4", "s5"}, NUM_GROUPS_LIMIT, false, true);
    // ARRAY_MAP_BASED (product 10^20 > Long.MAX_VALUE)
    compareDictionary(DICT_SV_COLUMNS, NUM_GROUPS_LIMIT, false, true);
  }

  @Test
  public void testDictionaryMultiValueVariants() {
    // ARRAY_BASED MV (product 100)
    compareDictionary(new String[]{M1}, NUM_GROUPS_LIMIT, true, false);
    // INT_MAP_BASED single MV column (~14_500 > arrayBasedThreshold): the in-place group id path
    compareDictionary(new String[]{M_HIGH}, NUM_GROUPS_LIMIT, true, true);
    // INT_MAP_BASED MV (product 10^6)
    compareDictionary(new String[]{M1, "s1", "s2"}, NUM_GROUPS_LIMIT, true, true);
    // LONG_MAP_BASED MV (product 10^10)
    compareDictionary(new String[]{M1, M2, "s1", "s2", "s3"}, NUM_GROUPS_LIMIT, true, true);
    // ARRAY_MAP_BASED MV (product 10^24)
    String[] arrayMapColumns = new String[DICT_SV_COLUMNS.length + 2];
    arrayMapColumns[0] = M1;
    arrayMapColumns[1] = M2;
    System.arraycopy(DICT_SV_COLUMNS, 0, arrayMapColumns, 2, DICT_SV_COLUMNS.length);
    compareDictionary(arrayMapColumns, NUM_GROUPS_LIMIT, true, true);
  }

  @Test
  public void testDictionaryCapSemantics() {
    // numGroupsLimit < cardinality product forces the map-based holders; group ids (including INVALID_ID
    // positions) must be identical
    compareDictionary(new String[]{"s1"}, 7, false, true);
    compareDictionary(new String[]{"s1", "s2", "s3", "s4", "s5"}, 7, false, true);
    compareDictionary(DICT_SV_COLUMNS, 7, false, true);
    compareDictionary(new String[]{M1}, 7, true, true);
  }

  // ---------------------------------------------------------------------------------------------
  // NoDictionarySingleColumnGroupKeyGenerator
  // ---------------------------------------------------------------------------------------------

  private RunResult[] compareNoDictionarySingle(String column, int numGroupsLimit, boolean nullHandlingEnabled,
      boolean multiValue, boolean primitiveNullDivergence) {
    String context = "NoDictionarySingle " + column + " limit " + numGroupsLimit
        + (nullHandlingEnabled ? " nullHandling" : "");
    ExpressionContext expression = ExpressionContext.forIdentifier(column);
    RunResult onHeap = run(op -> new NoDictionarySingleColumnGroupKeyGenerator(op, expression, numGroupsLimit,
        nullHandlingEnabled, null, false), multiValue);
    RunResult offHeap = run(op -> new NoDictionarySingleColumnGroupKeyGenerator(op, expression, numGroupsLimit,
        nullHandlingEnabled, null, true), multiValue);
    compareRuns(onHeap, offHeap, primitiveNullDivergence, true, context);
    return new RunResult[]{onHeap, offHeap};
  }

  @Test
  public void testNoDictionarySingleColumn() {
    for (String column : RAW_COLUMNS) {
      compareNoDictionarySingle(column, NUM_GROUPS_LIMIT, false, false, false);
    }
    // Dict-encoded column routed through the no-dict generator (as the executor does when null handling is on)
    compareNoDictionarySingle("s1", NUM_GROUPS_LIMIT, false, false, false);
  }

  @Test
  public void testNoDictionarySingleColumnMultiValue() {
    compareNoDictionarySingle(M1, NUM_GROUPS_LIMIT, false, true, false);
    compareNoDictionarySingle(M_HIGH, NUM_GROUPS_LIMIT, false, true, false);
  }

  @Test
  public void testNoDictionarySingleColumnCapSemantics() {
    compareNoDictionarySingle(R_INT, 7, false, false, false);
    compareNoDictionarySingle(R_STRING, 7, false, false, false);
    compareNoDictionarySingle(R_BIG_DECIMAL, 7, false, false, false);
  }

  @Test
  public void testNoDictionarySingleColumnNullHandling() {
    // Primitive stored types: off-heap counts must exceed on-heap by exactly 1 once the null group is assigned
    RunResult[] runs = compareNoDictionarySingle(NF_INT, NUM_GROUPS_LIMIT, true, false, true);
    // First row is null, so the null group must take dense id 0
    assertEquals(findNullGroupId(runs[1]._groupKeys), Integer.valueOf(0), "nfInt null group id");
    runs = compareNoDictionarySingle(NM_INT, NUM_GROUPS_LIMIT, true, false, true);
    // 8 distinct values (ids 0-7) precede the null stretch, so the null group must take dense id 8; the values
    // first appearing after the null stretch then shift to ids 9+
    assertEquals(findNullGroupId(runs[1]._groupKeys), Integer.valueOf(NULL_MID_GROUP_ID), "nmInt null group id");
    compareNoDictionarySingle(NF_DOUBLE, NUM_GROUPS_LIMIT, true, false, true);
    // Object stored types: the on-heap map holds the null key, so the counts must match exactly
    runs = compareNoDictionarySingle(NF_STR, NUM_GROUPS_LIMIT, true, false, false);
    assertEquals(findNullGroupId(runs[1]._groupKeys), Integer.valueOf(0), "nfStr null group id");
    runs = compareNoDictionarySingle(NM_STR, NUM_GROUPS_LIMIT, true, false, false);
    assertEquals(findNullGroupId(runs[1]._groupKeys), Integer.valueOf(NULL_MID_GROUP_ID), "nmStr null group id");
    compareNoDictionarySingle(NM_BIG_DECIMAL, NUM_GROUPS_LIMIT, true, false, false);
  }

  @Test
  public void testNoDictionarySingleColumnNullCapSemantics() {
    // Null group is the id that hits the cap: 8 value groups precede the null stretch, cap 9 -> null gets id 8
    // and every value first appearing after the null stretch gets INVALID_ID
    RunResult[] runs = compareNoDictionarySingle(NM_INT, NULL_MID_GROUP_ID + 1, true, false, true);
    assertEquals(findNullGroupId(runs[1]._groupKeys), Integer.valueOf(NULL_MID_GROUP_ID),
        "nmInt null group id at cap boundary");
    assertEquals(runs[1]._groupKeys.size(), NULL_MID_GROUP_ID + 1, "nmInt group count at cap boundary");
    // Cap hit before any null appears: 8 value groups fill a cap of 3 long before row 12_000, so the null group
    // must never be assigned (getKeyForNullValue returns INVALID_ID in both modes)
    runs = compareNoDictionarySingle(NM_INT, 3, true, false, true);
    assertNull(findNullGroupId(runs[1]._groupKeys), "nmInt cap-before-null must not assign a null group");
    assertEquals(runs[1]._groupKeys.size(), 3, "nmInt group count under cap 3");
    // Null in the very first row with a cap: null takes id 0, later new values are cut off by the cap
    runs = compareNoDictionarySingle(NF_INT, 7, true, false, true);
    assertEquals(findNullGroupId(runs[1]._groupKeys), Integer.valueOf(0), "nfInt null group id under cap");
    // Same cap scenarios for an object stored type
    runs = compareNoDictionarySingle(NM_STR, NULL_MID_GROUP_ID + 1, true, false, false);
    assertEquals(findNullGroupId(runs[1]._groupKeys), Integer.valueOf(NULL_MID_GROUP_ID),
        "nmStr null group id at cap boundary");
    runs = compareNoDictionarySingle(NM_STR, 3, true, false, false);
    assertNull(findNullGroupId(runs[1]._groupKeys), "nmStr cap-before-null must not assign a null group");
  }

  // ---------------------------------------------------------------------------------------------
  // NoDictionaryMultiColumnGroupKeyGenerator
  // ---------------------------------------------------------------------------------------------

  private RunResult[] compareNoDictionaryMulti(String[] columns, int numGroupsLimit, boolean nullHandlingEnabled,
      boolean multiValue) {
    String context = "NoDictionaryMulti " + Arrays.toString(columns) + " limit " + numGroupsLimit
        + (nullHandlingEnabled ? " nullHandling" : "");
    RunResult onHeap = run(op -> new NoDictionaryMultiColumnGroupKeyGenerator(op, getExpressions(columns),
        numGroupsLimit, nullHandlingEnabled, null, false), multiValue);
    RunResult offHeap = run(op -> new NoDictionaryMultiColumnGroupKeyGenerator(op, getExpressions(columns),
        numGroupsLimit, nullHandlingEnabled, null, true), multiValue);
    // The multi-column generator counts groups from the key map in both modes (null components are ID_FOR_NULL
    // inside the composite key), so there is never a counting divergence
    compareRuns(onHeap, offHeap, false, true, context);
    return new RunResult[]{onHeap, offHeap};
  }

  @Test
  public void testNoDictionaryMultiColumn() {
    compareNoDictionaryMulti(new String[]{R_INT, R_STRING}, NUM_GROUPS_LIMIT, false, false);
    compareNoDictionaryMulti(new String[]{R_FLOAT, R_DOUBLE}, NUM_GROUPS_LIMIT, false, false);
    compareNoDictionaryMulti(new String[]{R_LONG, R_BYTES, R_BIG_DECIMAL}, NUM_GROUPS_LIMIT, false, false);
    // Hybrid: dict-encoded column + raw column
    compareNoDictionaryMulti(new String[]{R_STRING, "s1"}, NUM_GROUPS_LIMIT, false, false);
  }

  @Test
  public void testNoDictionaryMultiColumnNullHandling() {
    RunResult[] runs = compareNoDictionaryMulti(new String[]{NF_INT, NM_STR}, NUM_GROUPS_LIMIT, true, false);
    // Sanity: composite groups with a null component must exist and contain nulls in the key positions
    boolean sawNullComponent = false;
    for (List<Object> keys : runs[1]._groupKeys.values()) {
      if (keys.get(0) == null || keys.get(1) == null) {
        sawNullComponent = true;
        break;
      }
    }
    assertTrue(sawNullComponent, "Expected composite groups with null components");
    compareNoDictionaryMulti(new String[]{NM_INT, NM_BIG_DECIMAL}, NUM_GROUPS_LIMIT, true, false);
  }

  @Test
  public void testNoDictionaryMultiColumnCapSemantics() {
    compareNoDictionaryMulti(new String[]{R_INT, R_STRING}, 7, false, false);
    compareNoDictionaryMulti(new String[]{NF_INT, NM_STR}, 5, true, false);
  }

  @Test
  public void testNoDictionaryMultiColumnMultiValue() {
    compareNoDictionaryMulti(new String[]{M1, R_INT}, NUM_GROUPS_LIMIT, false, true);
    compareNoDictionaryMulti(new String[]{M1, M2}, NUM_GROUPS_LIMIT, false, true);
  }

  @Test
  public void testNoDictionarySingleColumnDenseIdsWithNullPresent() {
    // Focused null-shift structural check: dense ids, no gaps, no duplicates, and the null group present exactly
    // once in the iterator, for both null layouts
    for (String column : new String[]{NF_INT, NM_INT, NF_STR, NM_STR}) {
      ExpressionContext expression = ExpressionContext.forIdentifier(column);
      RunResult offHeap = run(op -> new NoDictionarySingleColumnGroupKeyGenerator(op, expression, NUM_GROUPS_LIMIT,
          true, null, true), false);
      assertDenseIds(offHeap, column);
      assertNotNull(findNullGroupId(offHeap._groupKeys), column + ": null group missing from iterator");
      assertEquals(PinotDataBuffer.getDirectBufferUsage(), _directBufferBaseline,
          column + ": off-heap direct memory leaked");
    }
  }
}
