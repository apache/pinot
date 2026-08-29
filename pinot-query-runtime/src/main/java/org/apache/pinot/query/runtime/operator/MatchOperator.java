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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.MatchNode;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.match.MatchExpression;
import org.apache.pinot.query.runtime.operator.match.MatchLimits;
import org.apache.pinot.query.runtime.operator.match.MatchTape;
import org.apache.pinot.query.runtime.operator.match.PartitionMatcher;
import org.apache.pinot.query.runtime.operator.match.PatternNfa;
import org.apache.pinot.query.runtime.operator.match.PatternToNfaCompiler;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Evaluates SQL:2016 `MATCH_RECOGNIZE` (row pattern recognition) with `ONE ROW PER MATCH`.
///
/// ## What it does per partition
///
/// For every `PARTITION BY` partition, in `ORDER BY` order, it walks a scan position from the first row to
/// the last. At each position it asks [PartitionMatcher] for the preferred match starting exactly there. On a
/// match it emits one row - the partition key columns followed by the `MEASURES` - and then moves the scan
/// position according to the `AFTER MATCH SKIP` mode. On no match it moves one row forward.
///
/// ## What it expects from the plan
///
/// Like [WindowAggregateOperator], this operator does not sort. `PinotMatchExchangeNodeInsertRule` puts a
/// sort exchange underneath that hash distributes on the partition keys and sorts the receiver side on
/// `(partitionKeys..., orderKeys...)`, so rows arrive grouped by partition and ordered within a partition. The
/// operator therefore buffers one partition at a time and releases it at each boundary, and never reads
/// [MatchNode#getCollations()]: the ordering has already been established below it.
///
/// The grouping half of that assumption is verified rather than trusted. Partition keys are compared directly on the
/// incoming rows and must be monotonically increasing, so a key that reappears after its partition was closed fails
/// without retaining every previously seen key. The ordering half is not re-checked per row, because an exchange that
/// grouped correctly but sorted incorrectly is not a failure mode the exchange can produce - losing the sort loses the
/// grouping too, which the partition-key order check already catches.
///
/// ## Guardrails throw, they never truncate
///
/// [QueryOptionKey#MAX_ROWS_IN_MATCH_PARTITION] bounds the rows buffered for a partition and
/// [QueryOptionKey#MAX_STEPS_PER_MATCH_ATTEMPT] bounds the backtracking of one match attempt. Both raise an error,
/// because a truncated pattern result is a wrong result that nothing in the response would flag.
///
/// ## Not supported yet
///
/// `ALL ROWS PER MATCH` is rejected here as well as during planning: this operator emits exactly one row per
/// match, so accepting it would silently return the wrong shape of result.
public class MatchOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MatchOperator.class);
  private static final String EXPLAIN_NAME = "MATCH_RECOGNIZE";
  static final int MAX_OUTPUT_ROWS_PER_BLOCK = 1024;

  private final MultiStageOperator _input;
  private final DataSchema _resultSchema;
  private final ColumnDataType[] _resultStoredTypes;
  private final int[] _partitionKeys;
  private final int[] _partitionComparisonKeys;
  private final List<PatternSymbol> _patternSymbols;
  private final MatchExpression[] _measures;
  private final PartitionMatcher _matcher;
  private final MatchNode.AfterMatchSkipMode _skipMode;
  private final int _skipToSymbolOrdinal;
  private final int _maxRowsInMatchPartition;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  private List<Object[]> _partitionRows = new ArrayList<>();
  @Nullable
  private List<Object[]> _inputRows;
  private int _inputRowIndex;
  private boolean _matchingPartition;
  private int _scanStart;
  private long _matchNumber;
  @Nullable
  private MseBlock.Eos _inputEos;
  private int _numRows;

  public MatchOperator(OpChainExecutionContext context, MultiStageOperator input, DataSchema inputSchema,
      MatchNode node) {
    super(context);
    if (node.getRowsPerMatchMode() != MatchNode.RowsPerMatchMode.ONE_ROW_PER_MATCH) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "ALL ROWS PER MATCH is not supported yet in MATCH_RECOGNIZE. Use ONE ROW PER MATCH (the default) and "
              + "expose the per-match values you need through the MEASURES clause.");
    }
    _input = input;
    _resultSchema = node.getDataSchema();
    _resultStoredTypes = _resultSchema.getStoredColumnDataTypes();
    List<Integer> partitionKeys = node.getPartitionKeys();
    _partitionKeys = new int[partitionKeys.size()];
    for (int i = 0; i < _partitionKeys.length; i++) {
      _partitionKeys[i] = partitionKeys.get(i);
      ColumnDataType partitionKeyType = inputSchema.getColumnDataType(_partitionKeys[i]);
      if (partitionKeyType.isArray()) {
        throw QueryErrorCode.QUERY_EXECUTION.asException(
            "MATCH_RECOGNIZE PARTITION BY requires single-value columns, but column '"
                + inputSchema.getColumnName(_partitionKeys[i]) + "' has type " + partitionKeyType);
      }
    }
    // Calcite exposes the exchange distribution as an ImmutableBitSet, so the rule below this operator sorts
    // partition fields by input ordinal. Preserve SQL declaration order in _partitionKeys for the output schema, but
    // validate monotonic input against the order the exchange actually produces.
    _partitionComparisonKeys = _partitionKeys.clone();
    Arrays.sort(_partitionComparisonKeys);
    _patternSymbols = node.getPatternSymbols();
    List<MatchNode.Measure> measures = node.getMeasures();
    _measures = new MatchExpression[measures.size()];
    for (int i = 0; i < _measures.length; i++) {
      _measures[i] = MatchExpression.compile(measures.get(i).getExpression(), inputSchema);
    }
    if (_resultSchema.size() != _partitionKeys.length + _measures.length) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "MATCH_RECOGNIZE output schema " + _resultSchema + " does not match " + _partitionKeys.length
              + " partition key(s) plus " + _measures.length + " measure(s)");
    }
    _skipMode = node.getAfterMatchSkipMode();
    _skipToSymbolOrdinal = node.getAfterMatchSkipToSymbolOrdinal();

    PatternNfa nfa = PatternToNfaCompiler.compile(node.getPattern());
    _maxRowsInMatchPartition =
        MatchLimits.getMaxRowsInMatchPartition(context.getOpChainMetadata(), node.getNodeHint());
    long maxStepsPerMatchAttempt =
        MatchLimits.getMaxStepsPerMatchAttempt(context.getOpChainMetadata(), node.getNodeHint());
    _matcher = new PartitionMatcher(nfa, _patternSymbols, inputSchema, maxStepsPerMatchAttempt,
        this::checkTerminationAndSampleUsage);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_input);
  }

  @Override
  public Type getOperatorType() {
    return Type.MATCH;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(StatKey.GC_TIME_MS, gcTimeMs);
  }

  @Override
  public StatMap<StatKey> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  @Override
  protected MseBlock getNextBlock() {
    List<Object[]> outputRows = new ArrayList<>(MAX_OUTPUT_ROWS_PER_BLOCK);
    while (outputRows.size() < MAX_OUTPUT_ROWS_PER_BLOCK) {
      if (_matchingPartition) {
        emitMatches(outputRows);
        continue;
      }

      if (_inputRows != null) {
        consumeInputRowsUntilPartitionBoundary();
        continue;
      }

      if (_inputEos != null) {
        if (outputRows.isEmpty()) {
          return _inputEos;
        }
        break;
      }

      MseBlock block = _input.nextBlock();
      if (block.isData()) {
        _inputRows = ((MseBlock.Data) block).asRowHeap().getRows();
        _inputRowIndex = 0;
        continue;
      }

      _inputEos = (MseBlock.Eos) block;
      if (_inputEos.isError()) {
        return _inputEos;
      }
      if (!_partitionRows.isEmpty()) {
        startMatchingPartition();
      }
    }
    return new RowHeapDataBlock(outputRows, _resultSchema);
  }

  /// Consumes rows without allocating extracted keys. A boundary row stays in the input block until the preceding
  /// partition has been fully matched and emitted.
  private void consumeInputRowsUntilPartitionBoundary() {
    assert _inputRows != null;
    while (_inputRowIndex < _inputRows.size()) {
      Object[] row = _inputRows.get(_inputRowIndex);
      if (!_partitionRows.isEmpty()) {
        int comparison = comparePartitionKeys(_partitionRows.get(0), row);
        if (comparison > 0) {
          throw QueryErrorCode.QUERY_EXECUTION.asException(
              "MATCH_RECOGNIZE input is not grouped by partition key in ascending order: partition "
                  + partitionKeyToString(row) + " appeared after " + partitionKeyToString(_partitionRows.get(0)));
        }
        if (comparison < 0) {
          startMatchingPartition();
          return;
        }
      }
      if (_partitionRows.size() >= _maxRowsInMatchPartition) {
        throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
            "MATCH_RECOGNIZE partition exceeds the maximum of " + _maxRowsInMatchPartition
                + " rows. Partition on a column with more distinct values, or raise the '"
                + QueryOptionKey.MAX_ROWS_IN_MATCH_PARTITION + "' query option.");
      }
      _partitionRows.add(row);
      _inputRowIndex++;
      _numRows++;
      checkTerminationAndSampleUsagePeriodically(_numRows, EXPLAIN_NAME);
    }
    _inputRows = null;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private int comparePartitionKeys(Object[] leftRow, Object[] rightRow) {
    for (int partitionKey : _partitionComparisonKeys) {
      Object left = leftRow[partitionKey];
      Object right = rightRow[partitionKey];
      if (left == null) {
        if (right == null) {
          continue;
        }
        return 1;
      }
      if (right == null) {
        return -1;
      }
      if (!(left instanceof Comparable)) {
        throw QueryErrorCode.QUERY_EXECUTION.asException(
            "MATCH_RECOGNIZE PARTITION BY value is not sortable: " + left.getClass().getName());
      }
      final int comparison;
      try {
        comparison = ((Comparable) left).compareTo(right);
      } catch (ClassCastException e) {
        throw QueryErrorCode.QUERY_EXECUTION.asException(
            "MATCH_RECOGNIZE PARTITION BY values have incompatible types: " + left.getClass().getName() + " and "
                + right.getClass().getName());
      }
      if (comparison != 0) {
        return comparison;
      }
    }
    return 0;
  }

  private String partitionKeyToString(Object[] row) {
    StringBuilder builder = new StringBuilder("[");
    for (int i = 0; i < _partitionKeys.length; i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append(row[_partitionKeys[i]]);
    }
    return builder.append(']').toString();
  }

  private void startMatchingPartition() {
    _matchingPartition = true;
    _scanStart = 0;
    _matchNumber = 0;
  }

  /// Resumes scanning the buffered partition until it is exhausted or the current output block is full.
  private void emitMatches(List<Object[]> outputRows) {
    List<Object[]> rows = _partitionRows;
    int numPartitionRows = rows.size();
    MatchTape tape = _matcher.getTape();
    while (_scanStart < numPartitionRows && outputRows.size() < MAX_OUTPUT_ROWS_PER_BLOCK) {
      checkTerminationAndSampleUsage();
      int endPos = _matcher.match(rows, _scanStart, _matchNumber + 1);
      if (endPos == PartitionMatcher.NO_MATCH) {
        _scanStart++;
        continue;
      }
      _matchNumber++;
      outputRows.add(buildOutputRow(rows, tape));
      _scanStart = nextScanStart(_scanStart, endPos, tape);
    }
    if (_scanStart >= numPartitionRows) {
      _matcher.releasePartition();
      _partitionRows = new ArrayList<>();
      _matchingPartition = false;
    }
  }

  /// Builds the output row of one match: the partition key columns, which are constant across the partition, followed
  /// by the measures evaluated against the completed match.
  private Object[] buildOutputRow(List<Object[]> rows, MatchTape tape) {
    Object[] outputRow = new Object[_partitionKeys.length + _measures.length];
    Object[] anyPartitionRow = rows.get(0);
    for (int i = 0; i < _partitionKeys.length; i++) {
      outputRow[i] = anyPartitionRow[_partitionKeys[i]];
    }
    for (int i = 0; i < _measures.length; i++) {
      outputRow[_partitionKeys.length + i] = _measures[i].evaluate(tape);
    }
    TypeUtils.convertRow(outputRow, _resultStoredTypes);
    return outputRow;
  }

  /// Where pattern matching resumes after a match that covered `[scanStart, endPos)`.
  ///
  /// An empty match always resumes at the next row: every skip mode would otherwise resume exactly where it
  /// started and loop forever. `SKIP TO FIRST` / `SKIP TO LAST` that would not make progress is an error
  /// in SQL:2016 rather than a silently adjusted position, so it is reported as one.
  private int nextScanStart(int scanStart, int endPos, MatchTape tape) {
    if (endPos == scanStart) {
      if (_skipMode == MatchNode.AfterMatchSkipMode.TO_FIRST
          || _skipMode == MatchNode.AfterMatchSkipMode.TO_LAST) {
        throw QueryErrorCode.QUERY_EXECUTION.asException(
            "AFTER MATCH SKIP TO " + skipTargetName() + " cannot be applied to the empty match at row " + scanStart
                + " of its partition, because no row is mapped to that pattern variable. Make the PATTERN require "
                + "at least one row, or use AFTER MATCH SKIP PAST LAST ROW.");
      }
      return scanStart + 1;
    }
    switch (_skipMode) {
      case PAST_LAST_ROW:
        return endPos;
      case TO_NEXT_ROW:
        return scanStart + 1;
      case TO_FIRST:
        return skipToRow(tape.firstRow(_skipToSymbolOrdinal, 0), scanStart, "FIRST");
      case TO_LAST:
        return skipToRow(tape.lastRow(_skipToSymbolOrdinal, 0), scanStart, "LAST");
      default:
        throw QueryErrorCode.QUERY_EXECUTION.asException(
            "Unsupported MATCH_RECOGNIZE AFTER MATCH SKIP mode: " + _skipMode);
    }
  }

  private int skipToRow(int targetRow, int scanStart, String position) {
    if (targetRow == MatchTape.NO_ROW) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "AFTER MATCH SKIP TO " + position + " " + skipTargetName() + " failed: no row of the match starting at row "
              + scanStart + " of its partition is mapped to that pattern variable.");
    }
    if (targetRow <= scanStart) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "AFTER MATCH SKIP TO " + position + " " + skipTargetName()
              + " would resume at the first row of the match it just skipped, which would never terminate. Skip to a "
              + "pattern variable that cannot be mapped to the first row of the match, or use AFTER MATCH SKIP PAST "
              + "LAST ROW.");
    }
    return targetRow;
  }

  private String skipTargetName() {
    return _skipToSymbolOrdinal >= 0 && _skipToSymbolOrdinal < _patternSymbols.size()
        ? _patternSymbols.get(_skipToSymbolOrdinal).getName() : "<unknown>";
  }

  public enum StatKey implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    /// Allocated memory in bytes for this operator or its children in the same stage.
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    /// Time spent on GC while this operator or its children in the same stage were running.
    GC_TIME_MS(StatMap.Type.LONG);

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
