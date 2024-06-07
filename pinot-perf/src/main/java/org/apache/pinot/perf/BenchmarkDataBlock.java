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
import org.apache.pinot.core.common.datablock.DataBlockBuilderV2;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream;
import org.apache.pinot.spi.utils.ByteArray;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class BenchmarkDataBlock {

  private BenchmarkDataBlock() {
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

    builderConsumer.accept(opt);

    new Runner(opt.build()).run();
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
    private final CheckedFunction<List<ByteBuffer>, DataBlock> _deserialize;
    private final CheckedFunction<DataBlock, List<ByteBuffer>> _serialize;

    public BenchmarkState(int rows, DataSchema.ColumnDataType columnDataType, int nullPerCent,
        DataBlock.Type blockType)
        throws IOException {
      this(rows, columnDataType, "heap_small", nullPerCent, blockType);
    }

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
        _generateBlock = (data) -> DataBlockBuilderV2.buildFromColumns(data, _schema);
      } else {
        _generateBlock = (data) -> DataBlockBuilderV2.buildFromRows(data, _schema);
      }
      DataBlockUtils.setSerde(DataBlockSerde.Version.V1_V2, new ZeroCopyDataBlockSerde(alloc));
      _deserialize = DataBlockUtils::deserialize;
      _serialize = (dataBlock) -> DataBlockUtils.serialize(DataBlockSerde.Version.V1_V2, dataBlock);
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
          return r.nextInt();
        case LONG:
          return r.nextLong();
        case STRING:
          return "string" + r.nextInt(distinctStrings);
        case BYTES:
          byte[] bytes = new byte[100];
          r.nextBytes(bytes);
          return new ByteArray(bytes);
        case BIG_DECIMAL:
          return new BigDecimal(r.nextDouble());
        case BOOLEAN:
          return r.nextBoolean() ? 1 : 0;
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

    private List<ByteBuffer> serialize(DataBlock dataBlock)
        throws IOException {
      return _serialize.apply(dataBlock);
    }

    private DataBlock deserialize(List<ByteBuffer> buffers)
        throws IOException {
      return _deserialize.apply(buffers);
    }
  }

  interface CheckedFunction<I, O> {
    O apply(I input)
        throws IOException;
  }

  @Warmup(iterations = 2, time = 2)
  @Measurement(iterations = 5, time = 1)
  @Fork(value = 1, jvmArgsPrepend = {
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
  })
  @State(Scope.Benchmark)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class BuildBlock extends BenchmarkDataBlock {

    public static void main(String[] args)
        throws RunnerException {
      start(BenchmarkDataBlock.BuildBlock.class, opt ->
        opt
//            .addProfiler(LinuxPerfAsmProfiler.class)
//            .addProfiler(JavaFlightRecorderProfiler.class)
            .addProfiler(GCProfiler.class)
      );
    }

    @Param(value = {"INT", "LONG", "STRING", "BYTES", "BIG_DECIMAL", "BOOLEAN", "LONG_ARRAY", "STRING_ARRAY"})
    DataSchema.ColumnDataType _dataType;
    @Param(value = {"COLUMNAR", "ROW"})
    DataBlock.Type _blockType = DataBlock.Type.COLUMNAR;
    //    @Param(value = {"0", "10", "90"})
    int _nullPerCent = 10;

    @Param(value = {"10000", "1000000"})
    int _rows;

    BenchmarkState _state;

    @Setup
    public void setup()
        throws IOException {
      _state = new BenchmarkState(_rows, _dataType, _nullPerCent, _blockType);
    }

    @Benchmark
    public DataBlock buildBlock()
        throws IOException {
      return _state.createDataBlock();
    }
  }

  @Warmup(iterations = 2, time = 2)
  @Measurement(iterations = 5, time = 2)
  @Fork(value = 1, jvmArgsPrepend = {
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
  })
  @State(Scope.Benchmark)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class Serialize extends BenchmarkDataBlock {

    public static void main(String[] args)
        throws RunnerException {
      start(BenchmarkDataBlock.Serialize.class, opt ->
              opt
//            .addProfiler(LinuxPerfAsmProfiler.class)
//            .addProfiler(JavaFlightRecorderProfiler.class)
                  .addProfiler(GCProfiler.class)
      );
    }

    @Param(value = {"INT", "STRING", "BIG_DECIMAL", "LONG_ARRAY", "STRING_ARRAY"})
    DataSchema.ColumnDataType _columnDataType = DataSchema.ColumnDataType.INT;
    DataBlock.Type _blockType = DataBlock.Type.COLUMNAR;
    //    @Param(value = {"0", "10", "90"})
    int _nullPerCent = 10;
    @Param(value = {"direct_small", "heap_small"})
    String _version;

    @Param(value = {"10000", "1000000"})
    int _rows = 10000;

    BenchmarkState _state;

    @Setup
    public void setup()
        throws IOException {
      _state = new BenchmarkState(_rows, _columnDataType, _version, _nullPerCent, _blockType);
    }

    @Benchmark
    public Object serialize()
        throws IOException {
      return _state._serialize.apply(_state._dataBlock);
    }
  }

  @Warmup(iterations = 2, time = 2)
  @Measurement(iterations = 5, time = 2)
  @Fork(value = 1, jvmArgsPrepend = {
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
  })
  @State(Scope.Benchmark)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class Deserialize extends BenchmarkDataBlock {

    public static void main(String[] args)
        throws RunnerException {
      start(BenchmarkDataBlock.Deserialize.class, opt ->
              opt
//            .addProfiler(LinuxPerfAsmProfiler.class)
//            .addProfiler(JavaFlightRecorderProfiler.class)
                  .addProfiler(GCProfiler.class)
      );
    }

    @Param(value = {"INT", "STRING", "BIG_DECIMAL", "LONG_ARRAY", "STRING_ARRAY"})
    DataSchema.ColumnDataType _columnDataType = DataSchema.ColumnDataType.INT;
    DataBlock.Type _blockType = DataBlock.Type.COLUMNAR;
    int _nullPerCent = 10;
    @Param(value = {"direct_small", "heap_small"})
    String _version;

    @Param(value = {"10000", "1000000"})
    int _rows = 10000;

    BenchmarkState _state;

    @Setup
    public void setup()
        throws IOException {
      _state = new BenchmarkState(_rows, _columnDataType, _version, _nullPerCent, _blockType);
    }

    @Benchmark
    public DataBlock deserialize()
        throws IOException {
      return _state.deserialize(_state._bytes);
    }
  }

  @Warmup(iterations = 2, time = 4)
  @Measurement(iterations = 5, time = 1)
  @Fork(value = 1, jvmArgsPrepend = {
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
  })
  @State(Scope.Benchmark)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class BuildSerde extends BenchmarkDataBlock {

    public static void main(String[] args)
        throws RunnerException {
      start(BenchmarkDataBlock.BuildSerde.class, opt ->
              opt
//            .addProfiler(LinuxPerfAsmProfiler.class)
//            .addProfiler(JavaFlightRecorderProfiler.class)
                  .addProfiler(GCProfiler.class)
      );
    }

    @Param(value = {"INT", "LONG", "STRING", "BYTES", "BIG_DECIMAL", "BOOLEAN", "LONG_ARRAY", "STRING_ARRAY"})
    DataSchema.ColumnDataType _dataType;
    @Param(value = {"COLUMNAR", "ROW"})
    DataBlock.Type _blockType = DataBlock.Type.COLUMNAR;
    //    @Param(value = {"0", "10", "90"})
    int _nullPerCent = 10;

    @Param(value = {"direct_small", "heap_small"})
    String _version;

    @Param(value = {"10000", "1000000"})
    int _rows;

    BenchmarkState _state;

    @Setup
    public void setup()
        throws IOException {
      _state = new BenchmarkState(_rows, _dataType, _version, _nullPerCent, _blockType);
    }

    @Benchmark
    public DataBlock all()
        throws IOException {
      DataBlock dataBlock = _state.createDataBlock();
      List<ByteBuffer> buffers = _state.serialize(dataBlock);
      return _state.deserialize(buffers);
    }
  }
}
