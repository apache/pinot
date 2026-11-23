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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Compares the two ways to project a multi-stage row block to a subset of its columns, as `TransformOperator` does
/// when every projection is a column reference:
///
/// - `copy*`: one new `Object[]` per row, the path that `TransformOperator` used before.
/// - `view*`: [RowHeapDataBlock#project(int[], DataSchema)].
///
/// `*Serialize` builds the data block. `*Bytes` also writes it to the buffers that `GrpcSendingMailbox` sends, which is
/// everything a remote mailbox does with the block. `*Rows` is what a local receiver does: it reads the rows.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkMseRowProjection {

  public static void main(String[] args)
      throws RunnerException {
    new Runner(new OptionsBuilder().include(BenchmarkMseRowProjection.class.getSimpleName())
        .addProfiler(GCProfiler.class).build()).run();
  }

  @Param({"10000"})
  int _numRows;
  /// Number of columns of the input block. The projection keeps the first `_numProjectedColumns` in reverse order.
  @Param({"8"})
  int _numColumns;
  @Param({"2", "8"})
  int _numProjectedColumns;

  private List<Object[]> _rows;
  private DataSchema _inputSchema;
  private DataSchema _projectedSchema;
  private int[] _columnMap;

  @Setup
  public void setUp() {
    String[] names = new String[_numColumns];
    ColumnDataType[] types = new ColumnDataType[_numColumns];
    for (int i = 0; i < _numColumns; i++) {
      names[i] = "c" + i;
      types[i] = i % 2 == 0 ? ColumnDataType.LONG : ColumnDataType.STRING;
    }
    _inputSchema = new DataSchema(names, types);
    _columnMap = new int[_numProjectedColumns];
    String[] projectedNames = new String[_numProjectedColumns];
    ColumnDataType[] projectedTypes = new ColumnDataType[_numProjectedColumns];
    for (int i = 0; i < _numProjectedColumns; i++) {
      _columnMap[i] = _numProjectedColumns - 1 - i;
      projectedNames[i] = names[_columnMap[i]];
      projectedTypes[i] = types[_columnMap[i]];
    }
    _projectedSchema = new DataSchema(projectedNames, projectedTypes);
    _rows = new ArrayList<>(_numRows);
    for (int r = 0; r < _numRows; r++) {
      Object[] row = new Object[_numColumns];
      for (int c = 0; c < _numColumns; c++) {
        row[c] = c % 2 == 0 ? (Object) (long) (r * 31 + c) : "value-" + (r % 1000) + "-" + c;
      }
      _rows.add(row);
    }
  }

  private List<Object[]> copy() {
    List<Object[]> result = new ArrayList<>(_rows.size());
    for (Object[] row : _rows) {
      Object[] projected = new Object[_columnMap.length];
      for (int i = 0; i < _columnMap.length; i++) {
        projected[i] = row[_columnMap[i]];
      }
      result.add(projected);
    }
    return result;
  }

  private RowHeapDataBlock view() {
    return new RowHeapDataBlock(_rows, _inputSchema).project(_columnMap, _projectedSchema);
  }

  @Benchmark
  public DataBlock copySerialize()
      throws IOException {
    return DataBlockBuilder.buildFromRows(copy(), _projectedSchema);
  }

  @Benchmark
  public DataBlock viewSerialize() {
    return view().asSerialized().getDataBlock();
  }

  @Benchmark
  public List<ByteBuffer> copyBytes()
      throws IOException {
    return DataBlockBuilder.buildFromRows(copy(), _projectedSchema).serialize();
  }

  @Benchmark
  public List<ByteBuffer> viewBytes()
      throws IOException {
    return view().asSerialized().getDataBlock().serialize();
  }

  @Benchmark
  public List<Object[]> copyRows() {
    return new RowHeapDataBlock(copy(), _projectedSchema).getRows();
  }

  @Benchmark
  public List<Object[]> viewRows() {
    return view().getRows();
  }
}
