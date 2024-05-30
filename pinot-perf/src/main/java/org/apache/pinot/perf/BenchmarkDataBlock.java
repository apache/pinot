package org.apache.pinot.perf;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockSerde;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.OriginalDataBlockSerde;
import org.apache.pinot.common.datablock.ZeroCopyDataBlockSerde;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class BenchmarkDataBlock {

  public static class BenchmarkState {

    private final DataSchema.ColumnDataType _columnDataType;
    private final int _nullPerCent;
    private final DataBlock.Type _blockType;

    private final DataSchema _schema;
    private final List<Object[]> _data;
    private final DataBlock _dataBlock;
    private final ByteBuffer[] _bytes;

    private final CheckedSupplier<DataBlock> _deserialize;
    private final CheckedSupplier<Object> _serialize;

    public BenchmarkState(int rows, DataSchema.ColumnDataType columnDataType, String version, int nullPerCent,
        DataBlock.Type blockType)
        throws IOException {
      _nullPerCent = nullPerCent;
      _blockType = blockType;
      _columnDataType = columnDataType;

      _schema = new DataSchema(new String[]{"value"}, new DataSchema.ColumnDataType[]{columnDataType});
      _data = createData(rows);

      _dataBlock = createDataBlock();
      _bytes = new ByteBuffer[] {ByteBuffer.wrap(_dataBlock.toBytes())};

      int firstUnderscore = version.indexOf("_");
      String versionType;
      PagedPinotOutputStream.PageAllocator alloc;
      if (firstUnderscore == -1) {
        versionType = version;
        alloc = PagedPinotOutputStream.HeapPageAllocator.createSmall();
      } else {
        versionType = version.substring(0, firstUnderscore);
        switch (version.substring(firstUnderscore)) {
          case "_direct_small":
            alloc = PagedPinotOutputStream.DirectPageAllocator.createSmall(false);
            break;
          case "_direct_large":
            alloc = PagedPinotOutputStream.DirectPageAllocator.createLarge(false);
            break;
          case "_heap_small":
            alloc = PagedPinotOutputStream.HeapPageAllocator.createSmall();
            break;
          case "_heap_large":
            alloc = PagedPinotOutputStream.HeapPageAllocator.createLarge();
            break;
          default:
            throw new IllegalArgumentException("Cannot get allocator from version: " + version);
        }
      }

      switch (versionType) {
        case "bytes":
          _deserialize = () -> DataBlockUtils.getDataBlock(_bytes[0].duplicate());
          _serialize = () -> _dataBlock.toBytes();
          break;
        case "original":
          DataBlockUtils.setSerde(DataBlockSerde.Version.V2, new OriginalDataBlockSerde(alloc));
          _deserialize = () -> DataBlockUtils.deserialize(_bytes);
          _serialize = () -> DataBlockUtils.serialize(DataBlockSerde.Version.V2, _dataBlock);
          break;
        case "zero":
          DataBlockUtils.setSerde(DataBlockSerde.Version.V2, new ZeroCopyDataBlockSerde(alloc));
          _deserialize = () -> DataBlockUtils.deserialize(_bytes);
          _serialize = () -> DataBlockUtils.serialize(DataBlockSerde.Version.V2, _dataBlock);
          break;
        default:
          throw new IllegalArgumentException("Unsupported version: " + version);
      }
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
          return r.nextBoolean();
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
      switch (_blockType) {
        case COLUMNAR:
          return DataBlockBuilder.buildFromColumns(_data, _schema);
        case ROW:
          return DataBlockBuilder.buildFromRows(_data, _schema);
        default:
          throw new IllegalArgumentException("Unsupported data block type: " + _blockType);
      }
    }
  }

  interface CheckedSupplier<E> {
    E get()
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
      Options opt = new OptionsBuilder().include(BenchmarkDataBlock.BuildBlock.class.getSimpleName())
//          .addProfiler(LinuxPerfAsmProfiler.class)
//        .addProfiler(JavaFlightRecorderProfiler.class)
          .addProfiler(GCProfiler.class)
          .build();

      new Runner(opt).run();
    }

//    @Param(value = {"INT", "LONG", "STRING", "BYTES", "BIG_DECIMAL", "BOOLEAN", "LONG_ARRAY", "STRING_ARRAY"})
//    @Param(value = {"INT", "STRING", "BIG_DECIMAL", "LONG_ARRAY", "STRING_ARRAY"})
    DataSchema.ColumnDataType _columnDataType = DataSchema.ColumnDataType.INT;
    @Param(value = {"COLUMNAR", "ROW"})
    DataBlock.Type _blockType = DataBlock.Type.COLUMNAR;
//    @Param(value = {"0", "10", "90"})
    int _nullPerCent = 0;

    @Param(value = {"100", "10000", "1000000"})
    int _rows;

    BenchmarkState _state;
    @Setup
    public void setup()
        throws IOException {
      _state = new BenchmarkState(_rows, _columnDataType, "bytes", _nullPerCent, _blockType);
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
      Options opt = new OptionsBuilder().include(BenchmarkDataBlock.Serialize.class.getSimpleName())
//          .addProfiler(LinuxPerfAsmProfiler.class)
//        .addProfiler(JavaFlightRecorderProfiler.class)
          .addProfiler(GCProfiler.class)
          .build();

      new Runner(opt).run();
    }

//    @Param(value = {"INT", "STRING", "BYTES", "STRING_ARRAY"})
    DataSchema.ColumnDataType _columnDataType = DataSchema.ColumnDataType.INT;
    DataBlock.Type _blockType = DataBlock.Type.COLUMNAR;
    int _nullPerCent = 10;
    @Param(value = {"bytes", "zero_direct_small", "zero_heap_small"})
    String _version = "original_direct_small";

    @Param(value = {"100", "10000", "1000000"})
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
      return _state._serialize.get();
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
      Options opt = new OptionsBuilder().include(BenchmarkDataBlock.Deserialize.class.getSimpleName())
//          .addProfiler(LinuxPerfAsmProfiler.class)
//        .addProfiler(JavaFlightRecorderProfiler.class)
          .addProfiler(GCProfiler.class)
          .build();

      new Runner(opt).run();
    }

//    @Param(value = {"INT", "STRING", "BIG_DECIMAL", "LONG_ARRAY", "STRING_ARRAY"})
    DataSchema.ColumnDataType _columnDataType = DataSchema.ColumnDataType.INT;
    DataBlock.Type _blockType = DataBlock.Type.COLUMNAR;
    int _nullPerCent = 10;
    @Param(value = {"bytes", "original", "zero"})
    String _version = "zero";

    @Param(value = {"100", "10000", "1000000"})
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
      return _state._deserialize.get();
    }
  }
}
