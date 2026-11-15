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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Measures end-to-end offline segment creation (index writers, v3 conversion, metadata) with a mix of index types
/// typical of a real table: dictionary-encoded dimensions with inverted and range indexes, raw (no-dictionary) metrics
/// and strings, and a JSON index. Each invocation builds one segment into a fresh directory, so the score includes
/// every per-file close/sync the writers perform. The `b`/`kb`/`mb` counters report the final segment size on disk.
///
/// Rows are generated on the fly from a fixed seed (identical on every pass and every invocation), so memory stays flat
/// regardless of `_numRows`; row generation is part of the measured time for both baseline and candidate builds.
/// `_rawPayloadLength` sets the length of each raw string value and is the main lever for segment size. The benchmark
/// is single-threaded and the state is not shared between threads.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
public class BenchmarkSegmentCreation {
  private static final String TABLE_NAME = "segmentCreationBenchmark";
  private static final File ROOT_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkSegmentCreation");

  private static final int NUM_DICT_INT_COLUMNS = 10;
  private static final int NUM_INVERTED_INT_COLUMNS = 5;
  private static final int NUM_RANGE_INT_COLUMNS = 2;
  private static final int NUM_RAW_LONG_COLUMNS = 5;
  private static final int NUM_DICT_STRING_COLUMNS = 5;
  private static final int NUM_INVERTED_STRING_COLUMNS = 2;
  private static final int NUM_RAW_STRING_COLUMNS = 2;
  private static final String JSON_COLUMN = "j0";

  private static final Schema SCHEMA;
  private static final TableConfig TABLE_CONFIG;

  static {
    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME);
    List<String> invertedColumns = new ArrayList<>();
    List<String> rangeColumns = new ArrayList<>();
    List<String> noDictionaryColumns = new ArrayList<>();
    for (int i = 0; i < NUM_DICT_INT_COLUMNS; i++) {
      schemaBuilder.addSingleValueDimension("d" + i, FieldSpec.DataType.INT);
      if (i < NUM_INVERTED_INT_COLUMNS) {
        invertedColumns.add("d" + i);
      }
    }
    for (int i = 0; i < NUM_RANGE_INT_COLUMNS; i++) {
      schemaBuilder.addSingleValueDimension("r" + i, FieldSpec.DataType.INT);
      rangeColumns.add("r" + i);
    }
    for (int i = 0; i < NUM_RAW_LONG_COLUMNS; i++) {
      schemaBuilder.addMetric("m" + i, FieldSpec.DataType.LONG);
      noDictionaryColumns.add("m" + i);
    }
    for (int i = 0; i < NUM_DICT_STRING_COLUMNS; i++) {
      schemaBuilder.addSingleValueDimension("s" + i, FieldSpec.DataType.STRING);
      if (i < NUM_INVERTED_STRING_COLUMNS) {
        invertedColumns.add("s" + i);
      }
    }
    for (int i = 0; i < NUM_RAW_STRING_COLUMNS; i++) {
      schemaBuilder.addSingleValueDimension("t" + i, FieldSpec.DataType.STRING);
      noDictionaryColumns.add("t" + i);
    }
    schemaBuilder.addSingleValueDimension(JSON_COLUMN, FieldSpec.DataType.JSON);
    SCHEMA = schemaBuilder.build();
    TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(invertedColumns)
        .setRangeIndexColumns(rangeColumns)
        .setNoDictionaryColumns(noDictionaryColumns)
        .setJsonIndexColumns(List.of(JSON_COLUMN))
        .build();
  }

  @Param({"500000", "2000000"})
  int _numRows;

  @Param({"64", "512"})
  int _rawPayloadLength;

  private File _outDir;

  @Setup(Level.Iteration)
  public void createOutDir() {
    _outDir = new File(ROOT_DIR, UUID.randomUUID().toString());
  }

  @TearDown(Level.Iteration)
  public void deleteOutDir() {
    FileUtils.deleteQuietly(_outDir);
  }

  @TearDown(Level.Trial)
  public void deleteRootDir() {
    FileUtils.deleteQuietly(ROOT_DIR);
  }

  @Benchmark
  public File buildSegment(BytesCounter counter)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(_outDir.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(TABLE_NAME + "_" + _numRows);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (GeneratedRowReader recordReader = new GeneratedRowReader(_numRows, _rawPayloadLength)) {
      driver.init(config, recordReader);
      driver.build();
    }
    counter._bytes += FileUtils.sizeOfDirectory(_outDir);
    return _outDir;
  }

  /// Deterministic in-memory row generator: every pass (the driver reads the data twice) and every invocation sees
  /// identical rows, because [#rewind()] re-seeds the random source. Not thread-safe.
  private static final class GeneratedRowReader implements RecordReader {
    private static final long SEED = 42;
    private final int _numRows;
    private final int _rawPayloadLength;
    private final StringBuilder _payload = new StringBuilder();
    private SplittableRandom _random = new SplittableRandom(SEED);
    private int _nextRow;

    GeneratedRowReader(int numRows, int rawPayloadLength) {
      _numRows = numRows;
      _rawPayloadLength = rawPayloadLength;
    }

    @Override
    public void init(File dataFile, @Nullable Set<String> fieldsToRead,
        @Nullable RecordReaderConfig recordReaderConfig) {
    }

    @Override
    public boolean hasNext() {
      return _nextRow < _numRows;
    }

    @Override
    public GenericRow next(GenericRow reuse) {
      reuse.clear();
      SplittableRandom random = _random;
      for (int i = 0; i < NUM_DICT_INT_COLUMNS; i++) {
        reuse.putValue("d" + i, random.nextInt(1_000));
      }
      for (int i = 0; i < NUM_RANGE_INT_COLUMNS; i++) {
        reuse.putValue("r" + i, random.nextInt(100_000));
      }
      for (int i = 0; i < NUM_RAW_LONG_COLUMNS; i++) {
        reuse.putValue("m" + i, random.nextLong(1_000_000_000L));
      }
      for (int i = 0; i < NUM_DICT_STRING_COLUMNS; i++) {
        reuse.putValue("s" + i, "value-" + random.nextInt(10_000));
      }
      for (int i = 0; i < NUM_RAW_STRING_COLUMNS; i++) {
        reuse.putValue("t" + i, randomPayload(random));
      }
      reuse.putValue(JSON_COLUMN,
          "{\"service\":\"svc-" + random.nextInt(100) + "\",\"region\":\"r-" + random.nextInt(8) + "\",\"code\":"
              + random.nextInt(600) + "}");
      _nextRow++;
      return reuse;
    }

    /// Hex text of exactly `_rawPayloadLength` characters; mostly incompressible, like free-form ids and payloads.
    private String randomPayload(SplittableRandom random) {
      _payload.setLength(0);
      while (_payload.length() < _rawPayloadLength) {
        _payload.append(Long.toHexString(random.nextLong()));
      }
      _payload.setLength(_rawPayloadLength);
      return _payload.toString();
    }

    @Override
    public void rewind() {
      _random = new SplittableRandom(SEED);
      _nextRow = 0;
    }

    @Override
    public void close() {
    }
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkSegmentCreation.class.getSimpleName()).build()).run();
  }
}
