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

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockSerde;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.ZeroCopyDataBlockSerde;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream;
import org.apache.pinot.spi.utils.ByteArray;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsPrepend = {
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
})
@Threads(5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BenchmarkDataBlock {

  public static void main(String[] args)
      throws RunnerException {
    start(BenchmarkDataBlock.class, opt -> opt
//            .addProfiler(LinuxPerfAsmProfiler.class)
//            .addProfiler(JavaFlightRecorderProfiler.class)
        .addProfiler(GCProfiler.class));
  }

//  @Param(value = {"INT", "LONG", "STRING", "BYTES", "BIG_DECIMAL", "LONG_ARRAY", "STRING_ARRAY"})
  @Param(value = {"INT", "LONG", "STRING", "BYTES", "LONG_ARRAY"})
  DataSchema.ColumnDataType _dataType;
  @Param(value = {"COLUMNAR", "ROW"})
  DataBlock.Type _blockType = DataBlock.Type.COLUMNAR;
  //    @Param(value = {"0", "10", "90"})
  int _nullPerCent = 10;

  @Param(value = {"direct_small", "heap_small", "direct_large"})
  String _version = "heap_small";

//  @Param(value = {"10000", "1000000"})
  int _rows = 10000;

  BenchmarkState _state;

  @Setup(Level.Trial)
  public void setup()
      throws IOException {
    _state = new BenchmarkState(_rows, _dataType, _version, _nullPerCent, _blockType);
  }

  public static void start(Class<? extends BenchmarkDataBlock> benchmarkClass,
      Consumer<ChainedOptionsBuilder> builderConsumer)
      throws RunnerException {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(benchmarkClass.getSimpleName());

    File sdkJava = SystemUtils.getUserHome().toPath().resolve(".sdkman/candidates/java/current/bin/java").toFile();
    System.out.println("Java SDK: " + sdkJava);
    if (sdkJava.canExecute()) {
      opt.jvm(sdkJava.getAbsolutePath());
    }
    opt.resultFormat(ResultFormatType.CSV);

    builderConsumer.accept(opt);

    new Runner(opt.build()).run();
  }

  @Benchmark
  public DataBlock buildBlock()
      throws IOException {
    return _state.createDataBlock();
  }

  @Benchmark
  public Object serialize()
      throws IOException {
    return DataBlockUtils.serialize(DataBlockSerde.Version.V1_V2, _state._dataBlock);
  }

  @Benchmark
  public DataBlock deserialize()
      throws IOException {
    return DataBlockUtils.deserialize(_state._bytes);
  }

  @Benchmark
  public DataBlock all()
      throws IOException {
    DataBlock dataBlock = _state.createDataBlock();
    List<ByteBuffer> buffers = DataBlockUtils.serialize(dataBlock);
    return DataBlockUtils.deserialize(buffers);
  }

  public static class BenchmarkState {

    private final DataSchema.ColumnDataType _columnDataType;
    private final int _nullPerCent;
    private final DataBlock.Type _blockType;

    private final DataSchema _schema;
    private final List<Object[]> _data;
    private final DataBlock _dataBlock;
    private final List<ByteBuffer> _bytes;

    private final CheckedFunction<List<Object[]>, DataBlock> _generateBlock;

    public BenchmarkState(int rows, DataSchema.ColumnDataType columnDataType, String version, int nullPerCent,
        DataBlock.Type blockType)
        throws IOException {
      _nullPerCent = nullPerCent;
      _blockType = blockType;
      _columnDataType = columnDataType;

      _schema = new DataSchema(new String[]{"value"}, new DataSchema.ColumnDataType[]{columnDataType});
      _data = createData(rows);

      if (blockType == DataBlock.Type.COLUMNAR) {
        _dataBlock = DataBlockBuilder.buildFromColumns(_data, _schema);
      } else {
        _dataBlock = DataBlockBuilder.buildFromRows(_data, _schema);
      }
      _bytes = DataBlockUtils.serialize(_dataBlock);

      PagedPinotOutputStream.PageAllocator alloc;

      switch (version) {
        case "direct_small":
          alloc = PagedPinotOutputStream.DirectPageAllocator.createSmall(false);
          break;
        case "direct_large":
          alloc = PagedPinotOutputStream.DirectPageAllocator.createLarge(false);
          break;
        case "heap_small":
          alloc = PagedPinotOutputStream.HeapPageAllocator.createSmall();
          break;
        case "heap_large":
          alloc = PagedPinotOutputStream.HeapPageAllocator.createLarge();
          break;
        default:
          throw new IllegalArgumentException("Cannot get allocator from version: " + version);
      }

      if (blockType == DataBlock.Type.COLUMNAR) {
        _generateBlock = (data) -> DataBlockBuilder.buildFromColumns(data, _schema, alloc);
      } else {
        _generateBlock = (data) -> DataBlockBuilder.buildFromRows(data, _schema, alloc);
      }
      DataBlockUtils.setSerde(DataBlockSerde.Version.V1_V2, new ZeroCopyDataBlockSerde(alloc));
    }

    private List<Object[]> createData(int numRows) {
      Random r = new Random(42);
      switch (_blockType) {
        case COLUMNAR:
          Object[] column = new Object[numRows];
          for (int i = 0; i < numRows; i++) {
            column[i] = generateValue(r);
          }
          return Collections.singletonList(column);
        case ROW:
          ArrayList<Object[]> data = new ArrayList<>();
          for (int i = 0; i < numRows; i++) {
            data.add(new Object[]{generateValue(r)});
          }
          return data;
        default:
          throw new IllegalArgumentException("Unsupported data block type: " + _blockType);
      }
    }

    private Object generateValue(Random r) {
      if (r.nextInt(100) < _nullPerCent) {
        return null;
      }
      int distinctStrings = 100;
      switch (_columnDataType) {
        case INT:
          return (Object) r.nextInt();
        case LONG:
          return (Object) r.nextLong();
        case STRING:
          return "string" + r.nextInt(distinctStrings);
        case BYTES:
          byte[] bytes = new byte[100];
          r.nextBytes(bytes);
          return new ByteArray(bytes);
        case BIG_DECIMAL:
          return new BigDecimal(r.nextDouble());
        case BOOLEAN:
          return (Object) (r.nextBoolean() ? 1 : 0);
        case LONG_ARRAY:
          long[] longArray = new long[10];
          for (int i = 0; i < longArray.length; i++) {
            longArray[i] = r.nextLong();
          }
          return longArray;
        case STRING_ARRAY:
          String[] stringArray = new String[10];
          for (int i = 0; i < stringArray.length; i++) {
            stringArray[i] = "string" + r.nextInt(distinctStrings);
          }
          return stringArray;
        default:
          throw new IllegalArgumentException("Unsupported column data type: " + _columnDataType);
      }
    }

    private DataBlock createDataBlock()
        throws IOException {
      return _generateBlock.apply(_data);
    }
  }

  interface CheckedFunction<I, O> {
    O apply(I input)
        throws IOException;
  }
}
