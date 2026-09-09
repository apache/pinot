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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlockConverter;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.spi.utils.ByteArray;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Thread-confined codec and Arrow-native boundary comparisons over identical data. Run with {@code -prof gc};
 * codec-only legacy serialization reuses its prebuilt body, while the boundary cases include conversion costs.
 */
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsPrepend = "--add-opens=java.base/java.nio=ALL-UNNAMED")
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BenchmarkArrowIpc {
  @Param({"1024", "16384", "65536"})
  int _rows;

  @Param({"0", "10"})
  int _nullPercent;

  private RootAllocator _allocator;
  private BufferAllocator _decodeAllocator;
  private ArrowDataBlock _arrow;
  private ArrowBlock _arrowBlock;
  private DataBlock _legacy;
  private List<ByteBuffer> _arrowBytes;
  private List<ByteBuffer> _legacyBytes;

  @Setup(Level.Trial)
  public void setUp()
      throws IOException {
    _allocator = new RootAllocator(Long.MAX_VALUE);
    _decodeAllocator = _allocator.newChildAllocator("decode", 0, Long.MAX_VALUE);
    DataSchema schema = new DataSchema(new String[]{"key", "eventTime", "measure", "category", "payload"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.TIMESTAMP, ColumnDataType.DOUBLE,
            ColumnDataType.STRING, ColumnDataType.BYTES});
    DictionaryEncoding encoding = new DictionaryEncoding(37, false, new ArrowType.Int(32, true));
    Schema arrowSchema = new Schema(List.of(
        new Field("key", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("eventTime", FieldType.nullable(new ArrowType.Int(64, true)), null),
        new Field("measure", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
        new Field("category", new FieldType(true, new ArrowType.Int(32, true), encoding), null),
        new Field("payload", FieldType.nullable(new ArrowType.Binary()), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, _allocator);
    MapDictionaryProvider dictionaries = new MapDictionaryProvider();
    _arrow = new ArrowDataBlock(root, schema, dictionaries);
    _arrowBlock = new ArrowBlock(_arrow);
    VarCharVector values = new VarCharVector("categories", _allocator);
    dictionaries.put(new Dictionary(values, encoding));
    values.allocateNew(4096, 128);
    String[] categories = new String[128];
    for (int i = 0; i < categories.length; i++) {
      categories[i] = "category-" + i;
      values.setSafe(i, categories[i].getBytes(StandardCharsets.UTF_8));
    }
    values.setValueCount(categories.length);
    for (FieldVector vector : root.getFieldVectors()) {
      vector.setInitialCapacity(_rows);
    }
    root.allocateNew();
    List<Object[]> rows = new ArrayList<>(_rows);
    Random random = new Random(42);
    for (int row = 0; row < _rows; row++) {
      if (random.nextInt(100) < _nullPercent) {
        rows.add(new Object[5]);
        for (FieldVector vector : root.getFieldVectors()) {
          vector.setNull(row);
        }
      } else {
        int key = random.nextInt();
        long timestamp = 1700000000000L + row;
        double measure = random.nextDouble();
        int category = random.nextInt(categories.length);
        byte[] payload = new byte[32];
        random.nextBytes(payload);
        ((IntVector) root.getVector(0)).set(row, key);
        ((BigIntVector) root.getVector(1)).set(row, timestamp);
        ((Float8Vector) root.getVector(2)).set(row, measure);
        ((IntVector) root.getVector(3)).set(row, category);
        ((VarBinaryVector) root.getVector(4)).setSafe(row, payload);
        rows.add(new Object[]{key, timestamp, measure, categories[category], new ByteArray(payload)});
      }
    }
    root.setRowCount(_rows);
    _legacy = DataBlockBuilder.buildFromRows(rows, schema);
    _arrowBytes = DataBlockUtils.serialize(_arrow);
    _legacyBytes = DataBlockUtils.serialize(_legacy);
  }

  @Benchmark
  public List<ByteBuffer> arrowSerialize()
      throws IOException {
    return DataBlockUtils.serialize(_arrow);
  }

  @Benchmark
  public List<ByteBuffer> legacySerialize()
      throws IOException {
    return DataBlockUtils.serialize(_legacy);
  }

  @Benchmark
  public List<ByteBuffer> arrowToLegacySerialize()
      throws IOException {
    return DataBlockUtils.serialize(_arrowBlock.asSerialized().getDataBlock());
  }

  @Benchmark
  public int arrowDeserialize()
      throws IOException {
    try (ArrowDataBlock decoded = (ArrowDataBlock) DataBlockUtils.deserialize(_arrowBytes, _decodeAllocator)) {
      return decoded.getNumberOfRows();
    }
  }

  @Benchmark
  public DataBlock legacyDeserialize()
      throws IOException {
    return DataBlockUtils.deserialize(_legacyBytes);
  }

  @Benchmark
  public int legacyToArrowDeserialize()
      throws IOException {
    ArrowBlock decoded = ArrowBlockConverter.toArrowBlock(
        new SerializedDataBlock(DataBlockUtils.deserialize(_legacyBytes)), _decodeAllocator);
    try {
      return decoded.getNumRows();
    } finally {
      decoded.release();
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    _arrowBlock.release();
    _decodeAllocator.close();
    _allocator.close();
  }

  public static void main(String[] args)
      throws RunnerException, CommandLineOptionException {
    new Runner(new OptionsBuilder().parent(new CommandLineOptions(args))
        .include(BenchmarkArrowIpc.class.getSimpleName()).build()).run();
  }
}
