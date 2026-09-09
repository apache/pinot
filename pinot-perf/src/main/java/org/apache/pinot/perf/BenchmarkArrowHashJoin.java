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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlockConverter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.memory.ArrowBuffers;
import org.apache.pinot.query.runtime.memory.ArrowQueryContext;
import org.apache.pinot.query.runtime.operator.ArrowHashJoinOperator;
import org.apache.pinot.query.runtime.operator.HashJoinOperator;
import org.apache.pinot.query.runtime.operator.LiteralValueOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Isolates the selectivity/duplicate workloads of {@link BenchmarkEquiJoin} from cluster and network overhead.
 * Compares row execution, an Arrow-native region, and Arrow with both heap boundaries; use {@code -prof gc}.
 * Fixture blocks retain one reference throughout the trial; every invocation releases its own inputs and outputs.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsPrepend = {
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
})
@State(Scope.Thread)
public class BenchmarkArrowHashJoin {
  @Param({"65536"})
  public int _rows;
  @Param({"1024"})
  public int _blockRows;
  @Param({"1", "4"})
  public int _duplicates;
  @Param({"10", "80"})
  public int _matchPercent;
  @Param({"INT", "LONG", "FLOAT", "DOUBLE"})
  public String _keyType;
  @Param({"16"})
  public int _stringCardinality;

  private DataSchema _schema;
  private JoinNode _node;
  private ArrowBuffers _buffers;
  private ArrowQueryContext _arrow;
  private OpChainExecutionContext _context;
  private QueryThreadContext _threadContext;
  private List<MseBlock.Data> _leftRows;
  private List<MseBlock.Data> _rightRows;
  private List<MseBlock.Data> _leftArrow;
  private List<MseBlock.Data> _rightArrow;
  private long _expectedRows;

  @Setup(Level.Trial)
  public void setUp() {
    Preconditions.checkArgument(_rows > 0 && _duplicates > 0 && _rows % _duplicates == 0 && _blockRows > 0);
    Preconditions.checkArgument(_matchPercent >= 0 && _matchPercent <= 100);
    Preconditions.checkArgument(_stringCardinality > 0);
    ColumnDataType keyType = ColumnDataType.valueOf(_keyType);
    _schema = new DataSchema(new String[]{"key", "count", "value", "category"},
        new ColumnDataType[]{keyType, ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.STRING});
    DataSchema result = new DataSchema(
        new String[]{"lk", "lc", "lv", "ls", "rk", "rc", "rv", "rs"},
        new ColumnDataType[]{keyType, ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.STRING,
            keyType, ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.STRING});
    _node = new JoinNode(-1, result, PlanNode.NodeHint.EMPTY, List.of(), JoinRelType.INNER,
        List.of(0), List.of(0), List.of(), JoinNode.JoinStrategy.HASH);
    _buffers = new ArrowBuffers(true, new RootAllocator(512L * 1024 * 1024), 0, 512L * 1024 * 1024);
    MailboxService mailbox = Mockito.mock(MailboxService.class);
    Mockito.when(mailbox.isArrowEnabled()).thenReturn(true);
    Mockito.when(mailbox.getArrowBuffers()).thenReturn(_buffers);
    Mockito.when(mailbox.getHostname()).thenReturn("localhost");
    Mockito.when(mailbox.getPort()).thenReturn(1234);
    WorkerMetadata worker = new WorkerMetadata(0, Map.of(), Map.of());
    StageMetadata stage = new StageMetadata(0, List.of(worker), Map.of());
    _context = new OpChainExecutionContext(mailbox, 0, "benchmark", Long.MAX_VALUE, Long.MAX_VALUE, "broker",
        Map.of("maxRowsInJoin", "10000000"), stage, worker, null, false, false);
    _arrow = _context.getOrCreateArrowContext();
    _threadContext = QueryThreadContext.openForMseTest();
    _leftRows = makeRows(true, keyType);
    _rightRows = makeRows(false, keyType);
    _leftArrow = toArrow(_leftRows);
    _rightArrow = toArrow(_rightRows);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    for (MseBlock.Data block : _leftArrow) {
      ((ArrowBlock) block).release();
    }
    for (MseBlock.Data block : _rightArrow) {
      ((ArrowBlock) block).release();
    }
    try {
      Preconditions.checkState(_arrow.getLiveBlockCount() == 0, "Join invocation leaked Arrow blocks");
      Preconditions.checkState(_arrow.getAllocator().getAllocatedMemory() == 0, "Join invocation leaked Arrow buffers");
    } finally {
      try {
        _context.closeArrowResources();
      } finally {
        _buffers.close();
        _threadContext.close();
      }
    }
  }

  @Benchmark
  public long rowHashJoin(Blackhole blackhole) {
    return execute(false, false, blackhole);
  }

  @Benchmark
  public long arrowHashJoin(Blackhole blackhole) {
    return execute(true, false, blackhole);
  }

  @Benchmark
  public long arrowWithHeapBoundaries(Blackhole blackhole) {
    return execute(true, true, blackhole);
  }

  private long execute(boolean nativeJoin, boolean heapBoundaries, Blackhole blackhole) {
    MultiStageOperator left = new BlockInput(_context, nativeJoin && !heapBoundaries ? _leftArrow : _leftRows);
    MultiStageOperator right = new BlockInput(_context, nativeJoin && !heapBoundaries ? _rightArrow : _rightRows);
    MultiStageOperator join;
    if (nativeJoin) {
      ArrowHashJoinOperator arrow = new ArrowHashJoinOperator(_context, left, _schema, right, _schema, _node);
      if (!heapBoundaries) {
        arrow.enableArrowOutput();
      }
      join = arrow;
    } else {
      join = new HashJoinOperator(_context, left, _schema, right, _node);
    }
    long rows = 0;
    try (join) {
      while (true) {
        MseBlock block = join.nextBlock();
        if (block.isEos()) {
          Preconditions.checkState(block.isSuccess(), "Join failed: %s", block);
          break;
        }
        try {
          rows += ((MseBlock.Data) block).getNumRows();
          blackhole.consume(block);
        } finally {
          if (block instanceof ArrowBlock) {
            ((ArrowBlock) block).release();
          }
        }
      }
    }
    Preconditions.checkState(rows == _expectedRows, "Unexpected result cardinality: %s != %s", rows, _expectedRows);
    return rows;
  }

  private List<MseBlock.Data> makeRows(boolean probe, ColumnDataType keyType) {
    List<MseBlock.Data> blocks = new ArrayList<>();
    int distinctKeys = _rows / _duplicates;
    for (int start = 0; start < _rows; start += _blockRows) {
      int end = Math.min(start + _blockRows, _rows);
      List<Object[]> rows = new ArrayList<>(end - start);
      for (int row = start; row < end; row++) {
        boolean matches = row % 100 < _matchPercent;
        int key = probe && !matches ? distinctKeys + row : row % distinctKeys;
        Object value = switch (keyType) {
          case INT -> key;
          case LONG -> (long) key;
          case FLOAT -> (float) key;
          case DOUBLE -> (double) key;
          default -> throw new IllegalArgumentException("Unsupported benchmark key: " + keyType);
        };
        rows.add(new Object[]{value, (long) row, row * 0.25d, "category-" + row % _stringCardinality});
        if (probe && matches) {
          _expectedRows += _duplicates;
        }
      }
      blocks.add(new RowHeapDataBlock(rows, _schema));
    }
    return blocks;
  }

  private List<MseBlock.Data> toArrow(List<MseBlock.Data> rows) {
    List<MseBlock.Data> result = new ArrayList<>(rows.size());
    for (MseBlock.Data block : rows) {
      result.add(ArrowBlockConverter.toArrowBlock(block, _arrow));
    }
    return result;
  }

  public static void main(String[] args)
      throws RunnerException {
    new Runner(new OptionsBuilder().include(BenchmarkArrowHashJoin.class.getSimpleName())
        .addProfiler(GCProfiler.class).build()).run();
  }

  private static final class BlockInput extends MultiStageOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockInput.class);
    private final List<MseBlock.Data> _blocks;
    private int _next;

    private BlockInput(OpChainExecutionContext context, List<MseBlock.Data> blocks) {
      super(context);
      _blocks = blocks;
    }

    @Override
    protected MseBlock getNextBlock() {
      if (_next == _blocks.size()) {
        return SuccessMseBlock.INSTANCE;
      }
      MseBlock.Data block = _blocks.get(_next++);
      if (block instanceof ArrowBlock) {
        ((ArrowBlock) block).retain();
      }
      return block;
    }

    @Override
    public List<MultiStageOperator> getChildOperators() {
      return List.of();
    }

    @Override
    protected Logger logger() {
      return LOGGER;
    }

    @Override
    public Type getOperatorType() {
      return Type.LITERAL;
    }

    @Override
    public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    }

    @Override
    public StatMap<?> copyStatMaps() {
      return new StatMap<>(LiteralValueOperator.StatKey.class);
    }

    @Override
    public String toExplainString() {
      return "BENCHMARK_INPUT";
    }
  }
}
