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
package org.apache.pinot.broker.routing.segmentpruner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionInfo;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises request-local predicate preparation with real metadata initialization and refresh, without ZooKeeper.
public class SinglePartitionColumnSegmentPrunerTest {
  private static final String COLUMN = "memberId";
  private static final String TABLE = "testTable_OFFLINE";

  @Test
  public void testHashesOnceAcrossDistinctMetadataInstancesPerQuery() throws Exception {
    Map<String, ZNRecord> records = new LinkedHashMap<>();
    Set<String> expected = new HashSet<>();
    for (int i = 0; i < 128; i++) {
      String segment = "segment_" + i;
      records.put(segment, metadata(segment, "PrunerCounting", 8, Set.of(i % 8), Map.of("offset", "0")));
      if (i % 8 == 3) {
        expected.add(segment);
      }
    }
    SinglePartitionColumnSegmentPruner pruner = pruner(records);
    CountingPartitionFunction.CALLS.set(0);
    BrokerRequest request = request(predicate("EQUALS", "3"));
    assertEquals(pruner.prune(request, records.keySet()), expected);
    assertEquals(CountingPartitionFunction.CALLS.get(), 1);
    assertEquals(pruner.prune(request, records.keySet()), expected);
    assertEquals(CountingPartitionFunction.CALLS.get(), 2, "Computed hashes must not survive a prune call");
  }

  @Test
  public void testInterleavedFunctionConfigurationsAndPartitionCounts() throws Exception {
    Map<String, ZNRecord> records = new LinkedHashMap<>();
    records.put("a", metadata("a", "PrunerCounting", 8, Set.of(3), Map.of("offset", "0")));
    records.put("b", metadata("b", "PrunerCounting", 8, Set.of(4), Map.of("offset", "1")));
    records.put("c", metadata("c", "PrunerCounting", 8, Set.of(2), Map.of("offset", "0")));
    records.put("d", metadata("d", "PrunerCounting", 8, Set.of(3), Map.of("offset", "1")));
    records.put("e", metadata("e", "PrunerCounting", 16, Set.of(11), Map.of("offset", "0")));
    SinglePartitionColumnSegmentPruner pruner = pruner(records);
    CountingPartitionFunction.CALLS.set(0);
    assertEquals(pruner.prune(request(predicate("EQUALS", "11")), records.keySet()), Set.of("a", "b", "e"));
    assertEquals(CountingPartitionFunction.CALLS.get(), 3,
        "Interleaved segments with the same complete configuration must reuse hashes");
  }

  @Test
  public void testMixedFunctionsNormalizersAndFunctionConfig() throws Exception {
    Map<String, ZNRecord> moduloRecords = new LinkedHashMap<>();
    moduloRecords.put("positive", metadata("positive", "Modulo", 8, Set.of(7), null));
    moduloRecords.put("abs", metadata("abs", "Modulo", 8, Set.of(1), Map.of("partitionIdNormalizer", "ABS")));
    moduloRecords.put("wrongAbs", metadata("wrongAbs", "Modulo", 8, Set.of(7), Map.of("partitionIdNormalizer", "ABS")));
    assertEquals(pruner(moduloRecords).prune(request(predicate("EQUALS", "-1")), moduloRecords.keySet()),
        Set.of("positive", "abs"));

    String value = "80ff0102";
    Map<String, String> rawConfig = Map.of("useRawBytes", "true");
    int textPartition = PartitionFunctionFactory.getPartitionFunction("Murmur", 97, null).getPartition(value);
    int rawPartition = PartitionFunctionFactory.getPartitionFunction("Murmur", 97, rawConfig).getPartition(value);
    assertNotEquals(textPartition, rawPartition, "Fixture must distinguish raw-byte and string hashing");
    Map<String, ZNRecord> murmurRecords = new LinkedHashMap<>();
    murmurRecords.put("text", metadata("text", "Murmur", 97, Set.of(textPartition), null));
    murmurRecords.put("raw", metadata("raw", "Murmur", 97, Set.of(rawPartition), rawConfig));
    murmurRecords.put("wrongRaw", metadata("wrongRaw", "Murmur", 97, Set.of(textPartition), rawConfig));
    assertEquals(pruner(murmurRecords).prune(request(predicate("EQUALS", value)), murmurRecords.keySet()),
        Set.of("text", "raw"));

    Map<String, ZNRecord> lookupRecords = new LinkedHashMap<>();
    lookupRecords.put("first", metadata("first", "BoundedColumnValue", 3, Set.of(1),
        Map.of("columnValues", "11|12", "columnValuesDelimiter", "|")));
    lookupRecords.put("second", metadata("second", "BoundedColumnValue", 3, Set.of(2),
        Map.of("columnValues", "12|11", "columnValuesDelimiter", "|")));
    lookupRecords.put("modulo", metadata("modulo", "Modulo", 3, Set.of(2), null));
    assertEquals(pruner(lookupRecords).prune(request(predicate("EQUALS", "11")), lookupRecords.keySet()),
        lookupRecords.keySet());
  }

  @DataProvider
  public Object[][] candidateCounts() {
    return new Object[][]{{1}, {2}};
  }

  @Test(dataProvider = "candidateCounts")
  public void testAndOrUnsupportedPredicatesAndLazyInValues(int numSegments) throws Exception {
    Map<String, ZNRecord> records = new LinkedHashMap<>();
    for (int i = 0; i < numSegments; i++) {
      String segment = "segment_" + i;
      records.put(segment, metadata(segment, "Modulo", 8, Set.of(1), null));
    }
    SinglePartitionColumnSegmentPruner pruner = pruner(records);
    Set<String> segments = records.keySet();
    Expression invalidValue = predicate("EQUALS", "invalid-number");
    assertEquals(pruner.prune(request(predicate("IN", "1", "invalid-number")), segments), segments);
    expectThrows(NumberFormatException.class,
        () -> pruner.prune(request(predicate("IN", "2", "invalid-number")), segments));
    assertEquals(pruner.prune(request(function("AND", predicate("EQUALS", "2"), invalidValue)), segments), Set.of());
    assertEquals(pruner.prune(request(function("OR", predicate("EQUALS", "1"), invalidValue)), segments), segments);
    Expression invalidOperator = function("INVALID_OPERATOR");
    assertEquals(pruner.prune(request(function("OR", predicate("EQUALS", "1"), invalidOperator)), segments), segments);
    expectThrows(IllegalArgumentException.class, () -> pruner.prune(request(invalidOperator), segments));

    Expression unsupported = predicate("GREATER_THAN", "100");
    assertEquals(pruner.prune(request(function("AND", predicate("IN", "0", "1"), unsupported)), segments), segments);
    assertEquals(pruner.prune(request(function("OR", predicate("EQUALS", "2"), unsupported)), segments), segments);
    assertEquals(pruner.prune(request(function("NOT", predicate("EQUALS", "1"))), segments), segments);
    assertEquals(pruner.prune(request(function("EQUALS", RequestUtils.getIdentifierExpression("other"),
        RequestUtils.getLiteralExpression("invalid-number"))), segments), segments);
    Expression transformedColumn = function("LOWER", RequestUtils.getIdentifierExpression(COLUMN));
    assertEquals(pruner.prune(request(function("EQUALS", transformedColumn,
        RequestUtils.getLiteralExpression("invalid-number"))), segments), segments);
    BrokerRequest unfilteredRequest = new BrokerRequest();
    unfilteredRequest.setPinotQuery(new PinotQuery());
    assertSame(pruner.prune(unfilteredRequest, segments), segments);
  }

  @Test
  public void testEmptyCandidatesDoNotEvaluateFilter() throws Exception {
    SinglePartitionColumnSegmentPruner pruner = pruner(Map.of("one", metadata("one", "Modulo", 8, Set.of(1), null)));
    assertEquals(pruner.prune(request(function("INVALID_OPERATOR")), Set.of()), Set.of());
    assertEquals(pruner.prune(request(predicate("EQUALS", "invalid-number")), Set.of()), Set.of());
  }

  @Test
  public void testUnknownMetadataIsConservativeAndDoesNotEvaluateFilter() throws Exception {
    SinglePartitionColumnSegmentPruner pruner = new SinglePartitionColumnSegmentPruner(TABLE, COLUMN);
    ZNRecord invalid = new ZNRecord("invalid");
    invalid.setSimpleField(CommonConstants.Segment.PARTITION_METADATA, "invalid-json");
    pruner.init(null, null, List.of("missing", "empty", "invalid"),
        Arrays.asList(null, new ZNRecord("empty"), invalid));
    Set<String> segments = Set.of("missing", "empty", "invalid", "not-initialized");
    assertEquals(pruner.prune(request(predicate("EQUALS", "invalid-number")), segments), segments);
    assertEquals(pruner.prune(request(function("INVALID_OPERATOR")), segments), segments);
  }

  @Test(dataProvider = "candidateCounts")
  public void testRefreshUsesCurrentPartitionsAndConfiguration(int numSegments) throws Exception {
    Map<String, ZNRecord> records = new LinkedHashMap<>();
    for (int i = 0; i < numSegments; i++) {
      String segment = "segment_" + i;
      records.put(segment, metadata(segment, "PrunerCounting", 8, Set.of(3), Map.of("offset", "0")));
    }
    SinglePartitionColumnSegmentPruner pruner = pruner(records);
    Set<String> segments = records.keySet();
    BrokerRequest request = request(predicate("EQUALS", "3"));
    assertEquals(pruner.prune(request, segments), segments);
    for (String segment : segments) {
      pruner.refreshSegment(segment, metadata(segment, "PrunerCounting", 8, Set.of(4), Map.of("offset", "0")));
    }
    assertEquals(pruner.prune(request, segments), Set.of());
    for (String segment : segments) {
      pruner.refreshSegment(segment, metadata(segment, "PrunerCounting", 8, Set.of(4), Map.of("offset", "1")));
    }
    assertEquals(pruner.prune(request, segments), segments);
    for (String segment : segments) {
      pruner.refreshSegment(segment, null);
    }
    assertEquals(pruner.prune(request(predicate("EQUALS", "invalid-number")), segments), segments);
  }

  @Test
  public void testConcurrentQueriesKeepSeparatePreparedValues() throws Exception {
    Map<String, ZNRecord> records = new LinkedHashMap<>();
    for (int i = 0; i < 8; i++) {
      String segment = "segment_" + i;
      records.put(segment, metadata(segment, "Modulo", 8, Set.of(i), null));
    }
    SinglePartitionColumnSegmentPruner pruner = pruner(records);
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      List<Callable<Void>> queries = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        int partition = i % 8;
        queries.add(() -> {
          assertEquals(pruner.prune(request(predicate("EQUALS", Integer.toString(partition))), records.keySet()),
              Set.of("segment_" + partition));
          return null;
        });
      }
      for (Future<Void> future : executor.invokeAll(queries)) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testMetadataConfigurationSnapshotDistinguishesUnknownNullAndEmpty() {
    PartitionFunction function = new CountingPartitionFunction(8, null);
    SegmentPartitionInfo unknown = new SegmentPartitionInfo(COLUMN, function, Set.of(0));
    assertFalse(unknown.hasPartitionFunctionConfig());
    SegmentPartitionInfo knownNull = new SegmentPartitionInfo(COLUMN, function, Set.of(0), null);
    assertTrue(knownNull.hasPartitionFunctionConfig());
    assertNull(knownNull.getPartitionFunctionConfig());
    SegmentPartitionInfo knownEmpty = new SegmentPartitionInfo(COLUMN, function, Set.of(0), Map.of());
    assertEquals(knownEmpty.getPartitionFunctionConfig(), Map.of());
    Map<String, String> config = new HashMap<>();
    config.put("offset", "1");
    config.put("nullable", null);
    SegmentPartitionInfo snapshot = new SegmentPartitionInfo(COLUMN, function, Set.of(0), config);
    config.put("offset", "2");
    assertEquals(snapshot.getPartitionFunctionConfig().get("offset"), "1");
    assertTrue(snapshot.getPartitionFunctionConfig().containsKey("nullable"));
    expectThrows(UnsupportedOperationException.class, () -> snapshot.getPartitionFunctionConfig().put("offset", "3"));
  }

  private static SinglePartitionColumnSegmentPruner pruner(Map<String, ZNRecord> records) {
    SinglePartitionColumnSegmentPruner pruner = new SinglePartitionColumnSegmentPruner(TABLE, COLUMN);
    pruner.init(null, null, new ArrayList<>(records.keySet()), new ArrayList<>(records.values()));
    return pruner;
  }

  private static ZNRecord metadata(String segment, String function, int count, Set<Integer> partitions,
      @Nullable Map<String, String> config) throws Exception {
    ZNRecord record = new ZNRecord(segment);
    record.setSimpleField(CommonConstants.Segment.PARTITION_METADATA, new SegmentPartitionMetadata(Map.of(COLUMN,
        new ColumnPartitionMetadata(function, count, partitions, config))).toJsonString());
    return record;
  }

  private static Expression predicate(String operator, String... values) {
    List<Expression> operands = new ArrayList<>();
    operands.add(RequestUtils.getIdentifierExpression(COLUMN));
    for (String value : values) {
      operands.add(RequestUtils.getLiteralExpression(value));
    }
    return RequestUtils.getFunctionExpression(operator, operands);
  }

  private static Expression function(String operator, Expression... operands) {
    return RequestUtils.getFunctionExpression(operator, List.of(operands));
  }

  private static BrokerRequest request(Expression filter) {
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.setFilterExpression(filter);
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setPinotQuery(pinotQuery);
    return brokerRequest;
  }

  /// Stateless test plugin with observable hash calls; deliberately inherits the null function-config getter.
  public static class CountingPartitionFunction implements PartitionFunction {
    private static final long serialVersionUID = 1L;
    private static final AtomicInteger CALLS = new AtomicInteger();
    private final int _numPartitions;
    private final int _offset;

    public CountingPartitionFunction(int numPartitions, @Nullable Map<String, String> config) {
      _numPartitions = numPartitions;
      _offset = config == null ? 0 : Integer.parseInt(config.getOrDefault("offset", "0"));
    }

    @Override
    public int getPartition(String value) {
      CALLS.incrementAndGet();
      return Math.floorMod(Integer.parseInt(value) + _offset, _numPartitions);
    }

    @Override
    public String getName() {
      return "PrunerCounting";
    }

    @Override
    public int getNumPartitions() {
      return _numPartitions;
    }

    @Override
    public PartitionIdNormalizer getPartitionIdNormalizer() {
      return PartitionIdNormalizer.POSITIVE_MODULO;
    }
  }
}
