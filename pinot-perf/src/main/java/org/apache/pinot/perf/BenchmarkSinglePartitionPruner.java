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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionInfo;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionUtils;
import org.apache.pinot.broker.routing.segmentpartition.SinglePartitionInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.spi.partition.ModuloPartitionFunction;
import org.apache.pinot.segment.spi.partition.MurmurPartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.JavaFlightRecorderProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 1, time = 2)
@State(Scope.Benchmark)
public class BenchmarkSinglePartitionPruner {
  // @Param({"8", "64", "256"})
  @Param({"256"})
  private int _numPartitions;
  // @Param({"100", "1000", "10000", "10000"})
  @Param({"1000", "10000", "20000"})
  private int _numSegments;

  @Param({"forward", "inverted"})
  private String _pruneAlgorithm;

  @Param({"murmur"})
  private String _partitionFunction;

  @Param({"true", "false"})
  private boolean _badPartitioning;

  private SegmentPruner _pruner;
  private List<BrokerRequest> _queries;
  private final Set<String> _segments = new HashSet<>();
  private static final String QUERY_PREFIX = "SELECT * FROM testTable WHERE userId = ";

  private static final String PARTITION_COLULMN = "userId";

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkSinglePartitionPruner.class.getSimpleName());
    if (args.length > 0 && args[0].equals("jfr")) {
      opt = opt.addProfiler(JavaFlightRecorderProfiler.class)
          .jvmArgsAppend("-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints");
    }
    new Runner(opt.build()).run();
  }

  @Setup
  public void setBrokerRequest() {
    _queries = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      _queries.add(CalciteSqlCompiler.compileToBrokerRequest(QUERY_PREFIX + i));
    }
  }

  @Setup
  public void setupPruner() {
    HashMap<String, SegmentPartitionInfo> segmentPartitions = new HashMap<>();
    PartitionFunction partitionFunction;
    if (_partitionFunction.equals("murmur")) {
      partitionFunction = new MurmurPartitionFunction(_numPartitions);
    } else {
      partitionFunction = new ModuloPartitionFunction(_numPartitions);
    }
    for (int i = 0; i < _numSegments; i++) {
      _segments.add("Segment_" + i);
      Set<Integer> partitions = new HashSet<>(Set.of(_numSegments % _numPartitions));
      if (_badPartitioning) {
        for (int p = 0; p < _numPartitions; p++) {
          partitions.add(p);
        }
      }
      segmentPartitions.put(
          "Segment_" + i,
          new SegmentPartitionInfo(
              PARTITION_COLULMN, partitionFunction, partitions
          )
      );
    }

    if (_pruneAlgorithm.equals("inverted")) {
      _pruner = new InvertedSegmentPruner(PARTITION_COLULMN);
    } else {
      _pruner = new SinglePartitionColumnSegmentPruner(PARTITION_COLULMN);
    }
    _pruner.init(segmentPartitions);
  }

  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OperationsPerInvocation(500)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void prune() {
    for (BrokerRequest r : _queries) {
      Set<String> result = _pruner.prune(r, _segments);
      assert !result.isEmpty();
    }
  }
}

interface SegmentPruner {
  Set<String> prune(BrokerRequest brokerRequest, Set<String> segments);

  void init(Map<String, SegmentPartitionInfo> segmentPartitions);
}

class InvertedSegmentPruner implements SegmentPruner {
  private final String _partitionColumn;
  private final Map<SinglePartitionInfo, Set<String>> _partitionToSegmentMap = new ConcurrentHashMap<>();
  private final Set<String> _unPartitionedSegments = ConcurrentHashMap.newKeySet();

  public InvertedSegmentPruner(String partitionColumn) {
    _partitionColumn = partitionColumn;
  }

  @Override
  public void init(Map<String, SegmentPartitionInfo> segmentPartitions) {
    segmentPartitions.forEach(this::addSegmentToPartitionIndex);
  }

  private void addSegmentToPartitionIndex(String segment, SegmentPartitionInfo partitionInfo) {
    if (partitionInfo == null || partitionInfo.getPartitionColumn() == null) {
      _unPartitionedSegments.add(segment);
      return;
    }
    _unPartitionedSegments.remove(segment);
    Set<SinglePartitionInfo> singlePartitionInfos = partitionInfo.getPartitions()
        .stream()
        .map(p -> new SinglePartitionInfo(partitionInfo.getPartitionColumn(), partitionInfo.getPartitionFunction(), p))
        .collect(Collectors.toSet());
      for (SinglePartitionInfo singlePartitionInfo : singlePartitionInfos) {
        _partitionToSegmentMap.computeIfAbsent(singlePartitionInfo, k -> ConcurrentHashMap.newKeySet()).add(segment);
      }
      _partitionToSegmentMap.entrySet()
          .stream()
          .filter(entry -> !singlePartitionInfos.contains(entry.getKey()))
          .forEach(e -> e.getValue().remove(segment));
  }

  @Override
  public Set<String> prune(BrokerRequest brokerRequest, Set<String> segments) {
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    if (filterExpression == null) {
      return segments;
    }
    Set<String> selectedSegments = _partitionToSegmentMap.entrySet()
        .stream()
        .filter(entry -> isPartitionMatch(filterExpression, entry.getKey()))
        .map(Map.Entry::getValue)
        .flatMap(Set::stream)
        .filter(segments::contains)
        .collect(Collectors.toSet());
    _unPartitionedSegments.stream().filter(segments::contains).forEach(selectedSegments::add);
    return selectedSegments;
  }

  private boolean isPartitionMatch(Expression filterExpression, SinglePartitionInfo partitionInfo) {
    Function function = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();
    switch (filterKind) {
      case AND:
        for (Expression child : operands) {
          if (!isPartitionMatch(child, partitionInfo)) {
            return false;
          }
        }
        return true;
      case OR:
        for (Expression child : operands) {
          if (isPartitionMatch(child, partitionInfo)) {
            return true;
          }
        }
        return false;
      case EQUALS: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          return partitionInfo.getPartition() == partitionInfo.getPartitionFunction()
              .getPartition(RequestContextUtils.getStringValue(operands.get(1)));
        } else {
          return true;
        }
      }
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (partitionInfo.getPartition() == (partitionInfo.getPartitionFunction()
                .getPartition(RequestContextUtils.getStringValue(operands.get(i))))) {
              return true;
            }
          }
          return false;
        } else {
          return true;
        }
      }
      default:
        return true;
    }
  }
}

class SinglePartitionColumnSegmentPruner implements SegmentPruner {
  private final String _partitionColumn;
  private final Map<String, SegmentPartitionInfo> _partitionInfoMap = new ConcurrentHashMap<>();

  public SinglePartitionColumnSegmentPruner(String partitionColumn) {
    _partitionColumn = partitionColumn;
  }

  @Override
  public void init(Map<String, SegmentPartitionInfo> segmentPartitions) {
    _partitionInfoMap.putAll(segmentPartitions);
  }

  @Override
  public Set<String> prune(BrokerRequest brokerRequest, Set<String> segments) {
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    if (filterExpression == null) {
      return segments;
    }
    Set<String> selectedSegments = new HashSet<>();
    for (String segment : segments) {
      SegmentPartitionInfo partitionInfo = _partitionInfoMap.get(segment);
      if (partitionInfo == null || partitionInfo == SegmentPartitionUtils.INVALID_PARTITION_INFO || isPartitionMatch(
          filterExpression, partitionInfo)) {
        selectedSegments.add(segment);
      }
    }
    return selectedSegments;
  }

  private boolean isPartitionMatch(Expression filterExpression, SegmentPartitionInfo partitionInfo) {
    Function function = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();
    switch (filterKind) {
      case AND:
        for (Expression child : operands) {
          if (!isPartitionMatch(child, partitionInfo)) {
            return false;
          }
        }
        return true;
      case OR:
        for (Expression child : operands) {
          if (isPartitionMatch(child, partitionInfo)) {
            return true;
          }
        }
        return false;
      case EQUALS: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          return partitionInfo.getPartitions().contains(partitionInfo.getPartitionFunction()
              .getPartition(RequestContextUtils.getStringValue(operands.get(1))));
        } else {
          return true;
        }
      }
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (partitionInfo.getPartitions().contains(partitionInfo.getPartitionFunction()
                .getPartition(RequestContextUtils.getStringValue(operands.get(i))))) {
              return true;
            }
          }
          return false;
        } else {
          return true;
        }
      }
      default:
        return true;
    }
  }
}
