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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.queries.BaseQueriesTest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkTextMatchQueriesSSQE extends BaseQueriesTest {

  public static void main(String[] args)
      throws Exception {
    Options opt =
        new OptionsBuilder().include(BenchmarkTextMatchQueriesSSQE.class.getSimpleName())
            .shouldDoGC(true)
            .build();
    new Runner(opt).run();
  }

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkTextMatchQueriesSSQE");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME_TEMPLATE = "testSegment%d";
  private static final String SC_STR_COL_PREFIX = "SC_STR_COL_";
  private static final String MC_STR_COL_PREFIX = "MC_STR_COL_";
  private static final String TIMESTAMP_COL = "TSTMP_COL";

  private static final String AND = " AND ";
  private static final String OR = " OR ";

  private static final int COL_COUNT = 25;

  private static List<FieldConfig> _fieldConfigs;
  private static List<String> _singleColIdxCols;
  private static List<String> _multiColIdxCols;
  private static TableConfig _tableConfig;
  private static Schema _schema;

  @Param({/*"1",*/ "5"/*, "10", "50"*/})
  private int _numSegments;

  @Param("2000000")
  private int _numRows;

  @Param({"EXP(0.001)" /*,"EXP(0.5)", "EXP(0.999)"*/})
  String _scenario;

  @Param({"true", "false"})
  boolean _isMultiColIdx;

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  private static ThreadLocal<Distribution.DataSupplier> _suppliers;
  private static final AtomicInteger COUNTER = new AtomicInteger(42);
  private static final List<String[]> SEARCH_WORDS = Collections.synchronizedList(new ArrayList<>());
  private static final AtomicInteger SEARCH_COUNTER = new AtomicInteger(0);

  @Setup
  public void setUp()
      throws Exception {
    System.out.println("PID is " + ProcessHandle.current().pid());
    long start = System.currentTimeMillis();
    int workers = Math.min(Runtime.getRuntime().availableProcessors(), _numSegments);

    _suppliers = ThreadLocal.withInitial(() -> Distribution.createSupplier(COUNTER.incrementAndGet(), _scenario));
    FileUtils.deleteQuietly(INDEX_DIR);

    _fieldConfigs = new ArrayList<>();
    _multiColIdxCols = new ArrayList<>();
    _singleColIdxCols = new ArrayList<>();

    if (_isMultiColIdx) {
      for (int i = 0; i < COL_COUNT; i++) {
        String colName = MC_STR_COL_PREFIX + i;
        _multiColIdxCols.add(colName);
        _fieldConfigs.add(new FieldConfig.Builder(colName)
            .withEncodingType(FieldConfig.EncodingType.RAW).build());
      }
    } else {
      for (int i = 0; i < COL_COUNT; i++) {
        String colName = SC_STR_COL_PREFIX + i;
        _singleColIdxCols.add(colName);
        _fieldConfigs.add(new FieldConfig.Builder(colName)
            .withEncodingType(FieldConfig.EncodingType.RAW)
            .withIndexTypes(List.of(FieldConfig.IndexType.TEXT))
            .build());
      }
    }

    List<String> noDictCols = new ArrayList<>();
    noDictCols.add(TIMESTAMP_COL);
    noDictCols.addAll(_multiColIdxCols);

    _tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setFieldConfigList(_fieldConfigs)
        .setNoDictionaryColumns(noDictCols)
        .setMultiColumnTextIndexConfig(_isMultiColIdx ? new MultiColumnTextIndexConfig(_multiColIdxCols) : null)
        .build();

    Schema.SchemaBuilder builder = new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME);

    for (String strCol : _multiColIdxCols) {
      builder.addSingleValueDimension(strCol, FieldSpec.DataType.STRING);
    }
    for (String strCol : _singleColIdxCols) {
      builder.addSingleValueDimension(strCol, FieldSpec.DataType.STRING);
    }

    _schema = builder.addSingleValueDimension(TIMESTAMP_COL, FieldSpec.DataType.TIMESTAMP)
        .build();

    _indexSegments = Collections.synchronizedList(new ArrayList<>());
    final IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(_tableConfig, _schema);
    ExecutorService executors = Executors.newFixedThreadPool(workers, new ThreadFactory() {
      private final ThreadGroup _group;
      private final AtomicInteger _threadNumber = new AtomicInteger(1);
      private final String _namePrefix;

      {
        @SuppressWarnings("removal")
        SecurityManager s = System.getSecurityManager();
        _group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        _namePrefix = "thread-";
      }

      public Thread newThread(Runnable r) {
        Thread t = new Thread(_group, r, _namePrefix + _threadNumber.getAndIncrement(),
            0);
        if (!t.isDaemon()) {
          t.setDaemon(true);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
          t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
      }
    });
    List<Future<ImmutableSegment>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < _numSegments; i++) {
        final int segmentNum = i;
        futures.add(
            executors.submit(() -> {
              try {
                buildSegment(String.format(SEGMENT_NAME_TEMPLATE, segmentNum), _suppliers.get(), segmentNum);
                return ImmutableSegmentLoader.load(
                    new File(new File(INDEX_DIR, "" + segmentNum), String.format(SEGMENT_NAME_TEMPLATE, segmentNum)),
                    indexLoadingConfig);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }));
      }

      for (Future<ImmutableSegment> f : futures) {
        _indexSegments.add(f.get());
      }
    } finally {
      executors.shutdownNow();
    }

    _indexSegment = _indexSegments.get(0);
    long end = System.currentTimeMillis();

    System.out.println("Loading segments took " + (end - start) + " ms");
    System.gc();
  }

  private static final String[] WORDS = new String[]{"java", "database", "pinot", "distributed", "multi-tenant"};

  @TearDown
  public void tearDown() {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }

    FileUtils.deleteQuietly(INDEX_DIR);
    EXECUTOR_SERVICE.shutdownNow();
  }

  static LazyDataGenerator createTestData(int numRows, Distribution.DataSupplier supplier,
      final boolean isMultiColIdx) {
    //create data lazily to prevent OOM and speed up setup

    return new LazyDataGenerator() {
      private final Map<Integer, String> _strings = new HashMap<>();
      private Distribution.DataSupplier _supplier = supplier;
      private final StringBuilder _buffer = new StringBuilder();
      private final Random _random = new Random(1);
      private final Function<Integer, String> _function = k -> getString();
      private final int _range = 'z' - 'a' + 1;
      private final char[] _dict = new char[_range];

      {
        for (int i = 0, max = _range - 1; i < max; i++) {
          _dict[i] = (char) ('a' + i);
        }
        _dict[_range - 1] = ' ';
      }

      private void initStrings() {
        for (int i = 0; i < WORDS.length; i++) {
          _strings.put(i + 1, WORDS[i]);
        }
      }

      @Override
      public int size() {
        return numRows;
      }

      private String getString() {
        _buffer.setLength(0);
        for (int i = 0; i < 10; i++) {
          _buffer.append(_dict[_random.nextInt(_range)]);
        }
        return _buffer.toString();
      }

      @Override
      public GenericRow next(GenericRow row, int i) {
        for (String strCol : _multiColIdxCols) {
          String value = _strings.computeIfAbsent((int) _supplier.getAsLong(), _function);
          row.putValue(strCol, value);
        }
        for (String strCol : _singleColIdxCols) {
          row.putValue(strCol,
              _strings.computeIfAbsent((int) _supplier.getAsLong(), _function));
        }
        row.putValue(TIMESTAMP_COL, i * 1200 * 1000L);

        if (i % 100000 == 0) {
          String[] words = new String[10];
          for (int w = 0; w < 10; w++) {
            words[w] = (String) row.getValue(isMultiColIdx ? _multiColIdxCols.get(w) : _singleColIdxCols.get(w));
          }
          SEARCH_WORDS.add(words);
        }

        return null;
      }

      @Override
      public void rewind() {
        initStrings();
        _supplier.reset();
      }
    };
  }

  private void buildSegment(String segmentName, Distribution.DataSupplier supplier, int segmentNum)
      throws Exception {
    LazyDataGenerator rows = createTestData(_numRows, supplier, _isMultiColIdx);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _schema);
    String path = new File(INDEX_DIR, "" + segmentNum).getPath();
    System.out.println(
        "Bulding segment: " + segmentNum + " in path: " + path + " by thread: " + Thread.currentThread().getName());
    config.setOutDir(path);
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GeneratedDataRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
    //save generator state so that other segments are not identical to this one
    supplier.snapshot();
  }

  private String generateQuery(int cols, String joinOperator) {
    String columnPrefix = _isMultiColIdx ? MC_STR_COL_PREFIX : SC_STR_COL_PREFIX;

    StringBuilder buff = new StringBuilder("SELECT count(*) "
        + "FROM MyTable "
        + "WHERE ");

    String[] words = SEARCH_WORDS.get(SEARCH_COUNTER.incrementAndGet() % SEARCH_WORDS.size());

    for (int i = 0; i < cols; i++) {
      if (i > 0) {
        buff.append(joinOperator);
      }
      buff.append("TEXT_MATCH(")
          .append(columnPrefix).append(i)
          .append(", '").append(words[i]).append("') ");
    }

    return buff.toString();
  }

  private String generateMultiColQuery(int cols, String joinOperator) {
    String columnPrefix = MC_STR_COL_PREFIX;

    StringBuilder buff = new StringBuilder("SELECT count(*) "
        + "FROM MyTable "
        + "WHERE TEXT_MATCH(MC_STR_COL_0,'");

    String[] words = SEARCH_WORDS.get(SEARCH_COUNTER.incrementAndGet() % SEARCH_WORDS.size());

    for (int i = 0; i < cols; i++) {
      if (i > 0) {
        buff.append(joinOperator);
      }
      buff.append(columnPrefix).append(i).append(":\"").append(words[i]).append("\" ");
    }

    buff.append("') ");

    return buff.toString();
  }

  @Benchmark
  public BrokerResponseNative query1MultiCol() {
    return getBrokerResponse(generateQuery(1, AND));
  }

  @Benchmark
  public BrokerResponseNative query5MultiColAnd() {
    String query = _isMultiColIdx ? generateMultiColQuery(5, AND) : generateQuery(5, AND);
    return getBrokerResponse(query);
  }

  @Benchmark
  public BrokerResponseNative query5MultiColOr() {
    return getBrokerResponse(_isMultiColIdx ? generateMultiColQuery(5, OR) : generateQuery(5, OR));
  }

  @Benchmark
  public BrokerResponseNative query10MultiColAnd() {
    return getBrokerResponse(_isMultiColIdx ? generateMultiColQuery(10, AND) : generateQuery(10, AND));
  }

  @Benchmark
  public BrokerResponseNative query10MultiColOr() {
    return getBrokerResponse(_isMultiColIdx ? generateMultiColQuery(10, OR) : generateQuery(10, OR));
  }

  @Override
  protected String getFilter() {
    return null;
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }
}
