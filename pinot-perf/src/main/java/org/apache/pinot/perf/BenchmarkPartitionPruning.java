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
package org.apache.pinot.perf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentpruner.SinglePartitionColumnSegmentPruner;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.partition.function.MurmurPartitionFunction;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;


/// Measures a complete partition-pruning call over independently loaded segment metadata. SQL parsing and metadata
/// loading happen outside measurement; requests cycle through distinct values, so each invocation must prepare its own
/// predicate. Run with the JMH GC profiler to measure allocation as well as time per pruning call.
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
public class BenchmarkPartitionPruning {
  private static final int NUM_PARTITIONS = 128;
  private static final int NUM_QUERIES = 32;
  private static final String TABLE = "events_OFFLINE";
  private static final String COLUMN = "accountId";

  @Param({"1", "256", "4096"})
  public int _numSegments;

  @Param({"EQ", "IN_16", "IN_128", "UNRELATED"})
  public String _predicate;

  @Param({"HOMOGENEOUS", "INTERLEAVED"})
  public String _metadata;

  private SinglePartitionColumnSegmentPruner _pruner;
  private Set<String> _segments;
  private BrokerRequest[] _requests;
  private int _queryIndex;

  @Setup
  public void setup()
      throws Exception {
    _pruner = new SinglePartitionColumnSegmentPruner(TABLE, COLUMN);
    _segments = new LinkedHashSet<>();
    List<String> segmentNames = new ArrayList<>(_numSegments);
    List<ZNRecord> records = new ArrayList<>(_numSegments);
    for (int i = 0; i < _numSegments; i++) {
      String segment = "segment_" + i;
      segmentNames.add(segment);
      _segments.add(segment);
      int numPartitions = getNumPartitions(i);
      ColumnPartitionMetadata columnMetadata =
          new ColumnPartitionMetadata("Murmur", numPartitions, Set.of(i % numPartitions), null);
      ZNRecord record = new ZNRecord(segment);
      record.setSimpleField(CommonConstants.Segment.PARTITION_METADATA,
          new SegmentPartitionMetadata(Map.of(COLUMN, columnMetadata)).toJsonString());
      records.add(record);
    }
    _pruner.init(null, null, segmentNames, records);

    int numValues = switch (_predicate) {
      case "EQ", "UNRELATED" -> 1;
      case "IN_16" -> 16;
      case "IN_128" -> 128;
      default -> throw new IllegalArgumentException("Unknown predicate: " + _predicate);
    };
    PartitionFunction partitionFunction = new MurmurPartitionFunction(NUM_PARTITIONS, null);
    PartitionFunction otherPartitionFunction = new MurmurPartitionFunction(NUM_PARTITIONS * 2, null);
    _requests = new BrokerRequest[NUM_QUERIES];
    for (int query = 0; query < NUM_QUERIES; query++) {
      List<String> literals = new ArrayList<>(numValues);
      Set<Integer> matchingPartitions = new HashSet<>();
      Set<Integer> otherMatchingPartitions = new HashSet<>();
      for (int value = 0; value < numValues; value++) {
        String account = "account_" + (query * 1000 + value);
        literals.add("'" + account + "'");
        matchingPartitions.add(partitionFunction.getPartition(account));
        otherMatchingPartitions.add(otherPartitionFunction.getPartition(account));
      }
      String predicate = switch (_predicate) {
        case "EQ" -> COLUMN + " = " + literals.get(0);
        case "UNRELATED" -> "source = 'web'";
        default -> COLUMN + " IN (" + String.join(",", literals) + ")";
      };
      _requests[query] = CalciteSqlCompiler.compileToBrokerRequest(
          "SELECT COUNT(*) FROM events WHERE " + predicate + " AND eventTime >= 1000 AND eventTime < 2000");
      Set<String> expected = new HashSet<>();
      for (int i = 0; i < _numSegments; i++) {
        int numPartitions = getNumPartitions(i);
        Set<Integer> partitions = numPartitions == NUM_PARTITIONS ? matchingPartitions : otherMatchingPartitions;
        if (_predicate.equals("UNRELATED") || partitions.contains(i % numPartitions)) {
          expected.add("segment_" + i);
        }
      }
      if (!_pruner.prune(_requests[query], _segments).equals(expected)) {
        throw new IllegalStateException("Unexpected pruning result for request " + query);
      }
    }
  }

  private int getNumPartitions(int segmentIndex) {
    return _metadata.equals("INTERLEAVED") && (segmentIndex & 1) != 0 ? NUM_PARTITIONS * 2 : NUM_PARTITIONS;
  }

  @Benchmark
  public Set<String> prune() {
    BrokerRequest request = _requests[_queryIndex++ & (NUM_QUERIES - 1)];
    return _pruner.prune(request, _segments);
  }
}
