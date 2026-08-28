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
package org.apache.pinot.queries;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGeneratorContext;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGeneratorProvider;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Exercises the provider SPI through the real segment plan, projection, group-by, combine, serialization, and broker
/// reduction pipeline. The physical segment includes the three important forward-index shapes: dictionary encoded,
/// raw without a dictionary, and raw with a side dictionary.
public class GroupKeyGeneratorProviderQueriesTest extends BaseQueriesTest {
  private static final String TABLE_NAME = "providerTestTable";
  private static final String SEGMENT_NAME = "providerTestSegment";
  private static final String DICTIONARY_INT = "dictionaryInt";
  private static final String RAW_INT = "rawInt";
  private static final String RAW_LONG = "rawLong";
  private static final String RAW_SIDE_DICTIONARY_INT = "rawSideDictionaryInt";
  private static final String NULLABLE_RAW_INT = "nullableRawInt";
  private static final long RAW_LONG_MIN = Integer.MIN_VALUE - 1L;
  private static final long RAW_LONG_MAX = Integer.MAX_VALUE + 1L;
  private static final int PROVIDER_GROUP_LIMIT = 16;

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension(DICTIONARY_INT, DataType.INT)
      .addSingleValueDimension(RAW_INT, DataType.INT)
      .addSingleValueDimension(RAW_LONG, DataType.LONG)
      .addSingleValueDimension(RAW_SIDE_DICTIONARY_INT, DataType.INT)
      .addSingleValueDimension(NULLABLE_RAW_INT, DataType.INT)
      .build();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .setNoDictionaryColumns(List.of(RAW_INT, RAW_LONG, NULLABLE_RAW_INT))
      .setFieldConfigList(List.of(rawWithDictionary(RAW_SIDE_DICTIONARY_INT)))
      .setStarTreeIndexConfigs(List.of(
          new StarTreeIndexConfig(List.of(DICTIONARY_INT), null, List.of("COUNT__*"), null, 1)))
      .build();

  private File _indexDir;
  private ImmutableSegment _indexSegment;

  @BeforeClass
  public void setUp()
      throws Exception {
    _indexDir = Files.createTempDirectory(getClass().getSimpleName()).toFile();

    SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    generatorConfig.setOutDir(_indexDir.getAbsolutePath());
    generatorConfig.setSegmentName(SEGMENT_NAME);
    generatorConfig.setDefaultNullHandlingEnabled(true);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(createRows())) {
      driver.init(generatorConfig, recordReader);
      driver.build();
    }

    _indexSegment = ImmutableSegmentLoader.load(new File(_indexDir, SEGMENT_NAME), ReadMode.mmap);
    assertPhysicalIndexShapes();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    if (_indexSegment != null) {
      _indexSegment.destroy();
    }
    if (_indexDir != null) {
      FileUtils.deleteDirectory(_indexDir);
    }
  }

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return List.of(_indexSegment);
  }

  @DataProvider(name = "physicalGroupKeyShapes")
  public Object[][] physicalGroupKeyShapes() {
    return new Object[][]{
        {RAW_INT, DataType.INT, false, false, true, false, 10L, 30L, 3, List.of(10, 20, 30)},
        {RAW_LONG, DataType.LONG, false, false, false, false, RAW_LONG_MIN, RAW_LONG_MAX, 3,
            List.of(RAW_LONG_MIN, 0L, RAW_LONG_MAX)},
        {RAW_SIDE_DICTIONARY_INT, DataType.INT, false, false, true, true, 100L, 300L, 3, List.of(100, 200, 300)},
        {DICTIONARY_INT, DataType.INT, true, false, false, false, 1L, 3L, 3, List.of(1, 2, 3)},
        {NULLABLE_RAW_INT, DataType.INT, false, true, false, false, (long) Integer.MIN_VALUE, 9L, 3,
            Arrays.asList(null, 7, 9)}
    };
  }

  @Test(dataProvider = "physicalGroupKeyShapes")
  public void testProviderAgainstRealSegment(String column, DataType storedType, boolean dictionaryEncoded,
      boolean nullHandlingEnabled,
      boolean expectProviderSelection, boolean materializeSegmentResult, long expectedMin, long expectedMax,
      int expectedCardinality,
      List<Object> expectedKeys) {
    String query = "SET useStarTree=false; " + (nullHandlingEnabled ? "SET enableNullHandling=true; " : "")
        + "SELECT " + column + ", COUNT(*) FROM " + TABLE_NAME + " GROUP BY " + column
        + (materializeSegmentResult ? " ORDER BY " + column + " LIMIT 10" : "");
    BrokerResponseNative expectedResponse = getBrokerResponse(query);

    TrackingPlanMaker planMaker = new TrackingPlanMaker();
    BrokerResponseNative actualResponse = getBrokerResponse(query, planMaker);

    assertTrue(expectedResponse.getExceptions().isEmpty(), expectedResponse.getExceptions().toString());
    assertTrue(actualResponse.getExceptions().isEmpty(), actualResponse.getExceptions().toString());
    ResultTable expectedResultTable = expectedResponse.getResultTable();
    ResultTable actualResultTable = actualResponse.getResultTable();
    assertNotNull(expectedResultTable);
    assertNotNull(actualResultTable);
    assertEquals(actualResultTable.getDataSchema(), expectedResultTable.getDataSchema());
    Map<Object, Long> expectedCounts = toCountMap(expectedResultTable);
    assertEquals(toCountMap(actualResultTable), expectedCounts);
    assertEquals(expectedCounts.keySet(), new HashSet<>(expectedKeys));

    assertEquals(planMaker._contexts.size(), 1);
    GroupKeyGeneratorContext context = planMaker._contexts.get(0);
    assertEquals(context.getGroupKeySpecs().size(), 1);
    GroupKeyGeneratorContext.GroupKeySpec groupKeySpec = context.getGroupKeySpecs().get(0);
    assertEquals(groupKeySpec.expression().getIdentifier(), column);
    assertEquals(groupKeySpec.storedType(), storedType);
    assertTrue(groupKeySpec.singleValue());
    assertEquals(groupKeySpec.dictionaryEncoded(), dictionaryEncoded);
    assertEquals(context.isNullHandlingEnabled(), nullHandlingEnabled);
    assertEquals(context.getNumGroupsLimit(), PROVIDER_GROUP_LIMIT);
    assertEquals(context.getMaxInitialResultHolderCapacity(), PROVIDER_GROUP_LIMIT);
    assertEquals(groupKeySpec.exactIntegralDomain(),
        Optional.of(new GroupKeyGeneratorContext.IntegralDomain(expectedMin, expectedMax)));
    assertEquals(groupKeySpec.cardinalityHint(), OptionalInt.of(expectedCardinality));

    if (expectProviderSelection) {
      assertEquals(planMaker._generators.size(), 1);
      assertEquals(planMaker._generators.get(0)._closeAttempts.get(), 1);
    } else {
      assertTrue(planMaker._generators.isEmpty());
    }
  }

  @DataProvider(name = "providerExclusionQueries")
  public Object[][] providerExclusionQueries() {
    return new Object[][]{
        {"SET useStarTree=false; SELECT dictionaryInt, COUNT(*) FILTER(WHERE rawInt > 10) FROM " + TABLE_NAME
            + " GROUP BY dictionaryInt"},
        {"SET useStarTree=false; SET enableNullHandling=true; SELECT rawInt, COUNT(*) FROM " + TABLE_NAME
            + " GROUP BY ROLLUP(rawInt)"},
        {"SELECT dictionaryInt, COUNT(*) FROM " + TABLE_NAME + " GROUP BY dictionaryInt"}
    };
  }

  @Test(dataProvider = "providerExclusionQueries")
  public void testProviderExcludedFromSpecializedPlans(String query) {
    TrackingPlanMaker planMaker = new TrackingPlanMaker();
    BrokerResponseNative response = getBrokerResponse(query, planMaker);
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    assertNotNull(response.getResultTable());
    assertTrue(planMaker._contexts.isEmpty(), "Provider invoked for: " + query);
  }

  private void assertPhysicalIndexShapes() {
    assertNotNull(_indexSegment.getStarTrees());
    assertEquals(_indexSegment.getStarTrees().size(), 1);

    DataSource dictionaryDataSource = _indexSegment.getDataSource(DICTIONARY_INT);
    assertNotNull(dictionaryDataSource.getDictionary());
    assertTrue(dictionaryDataSource.getForwardIndex().isDictionaryEncoded());

    DataSource rawDataSource = _indexSegment.getDataSource(RAW_INT);
    assertNull(rawDataSource.getDictionary());
    assertFalse(rawDataSource.getForwardIndex().isDictionaryEncoded());

    DataSource rawLongDataSource = _indexSegment.getDataSource(RAW_LONG);
    assertNull(rawLongDataSource.getDictionary());
    assertFalse(rawLongDataSource.getForwardIndex().isDictionaryEncoded());

    DataSource rawSideDictionaryDataSource = _indexSegment.getDataSource(RAW_SIDE_DICTIONARY_INT);
    assertNotNull(rawSideDictionaryDataSource.getDictionary());
    assertFalse(rawSideDictionaryDataSource.getForwardIndex().isDictionaryEncoded());

    DataSource nullableRawDataSource = _indexSegment.getDataSource(NULLABLE_RAW_INT);
    assertNull(nullableRawDataSource.getDictionary());
    assertFalse(nullableRawDataSource.getForwardIndex().isDictionaryEncoded());
    assertNotNull(nullableRawDataSource.getNullValueVector());
    assertEquals(nullableRawDataSource.getNullValueVector().getNullBitmap().getCardinality(), 2);
  }

  private static List<GenericRow> createRows() {
    List<GenericRow> rows = new ArrayList<>();
    rows.add(row(1, 10, RAW_LONG_MIN, 100, 7));
    rows.add(row(2, 20, 0, 200, null));
    rows.add(row(1, 10, RAW_LONG_MIN, 100, 7));
    rows.add(row(3, 30, RAW_LONG_MAX, 300, 9));
    rows.add(row(2, 20, 0, 200, null));
    rows.add(row(3, 30, RAW_LONG_MAX, 300, 9));
    return rows;
  }

  private static GenericRow row(int dictionaryValue, int rawValue, long rawLongValue, int rawSideDictionaryValue,
      Integer nullableRawValue) {
    GenericRow row = new GenericRow();
    row.putValue(DICTIONARY_INT, dictionaryValue);
    row.putValue(RAW_INT, rawValue);
    row.putValue(RAW_LONG, rawLongValue);
    row.putValue(RAW_SIDE_DICTIONARY_INT, rawSideDictionaryValue);
    if (nullableRawValue == null) {
      row.putDefaultNullValue(NULLABLE_RAW_INT, SCHEMA.getFieldSpecFor(NULLABLE_RAW_INT).getDefaultNullValue());
    } else {
      row.putValue(NULLABLE_RAW_INT, nullableRawValue);
    }
    return row;
  }

  private static FieldConfig rawWithDictionary(String column) {
    ObjectNode indexes = JsonUtils.newObjectNode();
    ObjectNode forwardIndex = JsonUtils.newObjectNode();
    forwardIndex.put("encodingType", "RAW");
    indexes.set("forward", forwardIndex);
    ObjectNode dictionaryIndex = JsonUtils.newObjectNode();
    dictionaryIndex.put("disabled", false);
    indexes.set("dictionary", dictionaryIndex);
    return new FieldConfig.Builder(column).withEncodingType(FieldConfig.EncodingType.RAW).withIndexes(indexes).build();
  }

  private static Map<Object, Long> toCountMap(ResultTable resultTable) {
    Map<Object, Long> counts = new LinkedHashMap<>();
    for (Object[] row : resultTable.getRows()) {
      Object key = row[0];
      assertFalse(counts.containsKey(key), "Duplicate group key: " + key);
      counts.put(key, ((Number) row[1]).longValue());
    }
    return counts;
  }

  private static final class TrackingPlanMaker extends InstancePlanMakerImplV2 {
    private final List<GroupKeyGeneratorContext> _contexts = new CopyOnWriteArrayList<>();
    private final List<TrackingIntGroupKeyGenerator> _generators = new CopyOnWriteArrayList<>();

    private TrackingPlanMaker() {
      setNumGroupsLimit(PROVIDER_GROUP_LIMIT);
      setMaxInitialResultHolderCapacity(PROVIDER_GROUP_LIMIT);
    }

    @Override
    protected GroupKeyGeneratorProvider getGroupKeyGeneratorProvider(SegmentContext segmentContext,
        QueryContext queryContext) {
      return context -> {
        _contexts.add(context);
        List<GroupKeyGeneratorContext.GroupKeySpec> groupKeySpecs = context.getGroupKeySpecs();
        if (groupKeySpecs.size() != 1 || context.isNullHandlingEnabled()) {
          return Optional.empty();
        }
        GroupKeyGeneratorContext.GroupKeySpec groupKeySpec = groupKeySpecs.get(0);
        if (groupKeySpec.storedType() != DataType.INT || !groupKeySpec.singleValue()
            || groupKeySpec.dictionaryEncoded()) {
          return Optional.empty();
        }
        TrackingIntGroupKeyGenerator generator =
            new TrackingIntGroupKeyGenerator(groupKeySpec.expression(), context.getNumGroupsLimit());
        _generators.add(generator);
        return Optional.of(generator);
      };
    }
  }

  private static final class TrackingIntGroupKeyGenerator implements GroupKeyGenerator {
    private final ExpressionContext _expression;
    private final int _numGroupsLimit;
    private final Map<Integer, Integer> _groupIds = new LinkedHashMap<>();
    private final AtomicInteger _closeAttempts = new AtomicInteger();

    private TrackingIntGroupKeyGenerator(ExpressionContext expression, int numGroupsLimit) {
      _expression = expression;
      _numGroupsLimit = numGroupsLimit;
    }

    @Override
    public int getGlobalGroupKeyUpperBound() {
      return _numGroupsLimit;
    }

    @Override
    public void generateKeysForBlock(ValueBlock valueBlock, int[] groupKeys) {
      int[] values = valueBlock.getBlockValueSet(_expression).getIntValuesSV();
      int numDocs = valueBlock.getNumDocs();
      for (int i = 0; i < numDocs; i++) {
        Integer groupId = _groupIds.get(values[i]);
        if (groupId == null) {
          if (_groupIds.size() >= _numGroupsLimit) {
            groupKeys[i] = INVALID_ID;
            continue;
          }
          groupId = _groupIds.size();
          _groupIds.put(values[i], groupId);
        }
        groupKeys[i] = groupId;
      }
    }

    @Override
    public void generateKeysForBlock(ValueBlock valueBlock, int[][] groupKeys) {
      throw new AssertionError("Single-value provider should not use the multi-value path");
    }

    @Override
    public int getCurrentGroupKeyUpperBound() {
      return _groupIds.size();
    }

    @Override
    public Iterator<GroupKey> getGroupKeys() {
      Iterator<Map.Entry<Integer, Integer>> entries = _groupIds.entrySet().iterator();
      return new Iterator<>() {
        @Override
        public boolean hasNext() {
          return entries.hasNext();
        }

        @Override
        public GroupKey next() {
          Map.Entry<Integer, Integer> entry = entries.next();
          GroupKey groupKey = new GroupKey();
          groupKey._groupId = entry.getValue();
          groupKey._keys = new Object[]{entry.getKey()};
          return groupKey;
        }
      };
    }

    @Override
    public int getNumKeys() {
      return _groupIds.size();
    }

    @Override
    public void close() {
      _closeAttempts.incrementAndGet();
    }
  }
}
