package org.apache.pinot.perf;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.queries.BaseQueriesTest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.SegmentAllIndexPreprocessThrottler;
import org.apache.pinot.segment.local.utils.SegmentDownloadThrottler;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottler;
import org.apache.pinot.segment.local.utils.SegmentStarTreePreprocessThrottler;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkDimensionTableOverhead extends BaseQueriesTest {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(BenchmarkDimensionTableOverhead.class.getSimpleName())
        .shouldDoGC(false)
        .addProfiler(GCProfiler.class);
    new Runner(opt.build()).run();
  }

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FilteredAggregationsTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME_TEMPLATE = "testSegment%d";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String SORTED_COL_NAME = "SORTED_COL";
  private static final String RAW_INT_COL_NAME = "RAW_INT_COL";
  private static final String RAW_STRING_COL_NAME = "RAW_STRING_COL";
  private static final String NO_INDEX_INT_COL_NAME = "NO_INDEX_INT_COL";
  private static final String NO_INDEX_STRING_COL = "NO_INDEX_STRING_COL";
  private static final String LOW_CARDINALITY_STRING_COL = "LOW_CARDINALITY_STRING_COL";
  private static final String TIMESTAMP_COL = "TSTMP_COL";
  private static final List<FieldConfig> FIELD_CONFIGS = new ArrayList<>();

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(SORTED_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(NO_INDEX_INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(RAW_INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(RAW_STRING_COL_NAME, FieldSpec.DataType.STRING)
      .addSingleValueDimension(NO_INDEX_STRING_COL, FieldSpec.DataType.STRING)
      .addSingleValueDimension(LOW_CARDINALITY_STRING_COL, FieldSpec.DataType.STRING)
      .addSingleValueDimension(TIMESTAMP_COL, FieldSpec.DataType.TIMESTAMP)
      .setPrimaryKeyColumns(Arrays.asList(SORTED_COL_NAME, RAW_STRING_COL_NAME, NO_INDEX_STRING_COL, RAW_INT_COL_NAME))
      .build();

  private static final SegmentOperationsThrottler SEGMENT_OPERATIONS_THROTTLER = new SegmentOperationsThrottler(
      new SegmentAllIndexPreprocessThrottler(1, 2, true),
      new SegmentStarTreePreprocessThrottler(1, 2, true),
      new SegmentDownloadThrottler(1, 2, true));

  @Param({"1"})
  private int _numSegments;

  @Param("3000000")
  private int _numRows;

  @Param({"EXP(0.001)"})
  String _scenario;

  private static int _iteration = 0;

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private Distribution.DataSupplier _supplier;
  private DimensionTableDataManager _tableDataManager;

  @Setup(Level.Iteration)
  public void setUp()
      throws Exception {
    _supplier = Distribution.createSupplier(42, _scenario);
    FileUtils.deleteQuietly(INDEX_DIR);

    _indexSegments = new ArrayList<>();
    TableConfig tableConfig = getTableConfig(false);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);
    for (int i = 0; i < _numSegments; i++) {
      buildSegment(String.format(SEGMENT_NAME_TEMPLATE, i), tableConfig);
      _indexSegments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, String.format(SEGMENT_NAME_TEMPLATE, i)),
          indexLoadingConfig));
    }
    _indexSegment = _indexSegments.get(0);

    System.gc();
  }

  @Benchmark
  public DimensionTableDataManager benchmarkFastTableManager() {
    HelixManager helixManager = Mockito.mock(HelixManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    Mockito.when(propertyStore.get("/SCHEMAS/" + TABLE_NAME, null, AccessOption.PERSISTENT)).thenReturn(
        SchemaUtils.toZNRecord(SCHEMA));
    Mockito.when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

    InstanceDataManagerConfig instanceDataManagerConfig = Mockito.mock(InstanceDataManagerConfig.class);
    Mockito.when(instanceDataManagerConfig.getInstanceDataDir())
        .thenReturn(INDEX_DIR.getParentFile().getAbsolutePath());

    TableConfig tableConfig = getTableConfig(false);

    String tableName = TABLE_NAME + "_" + _iteration;
    _tableDataManager = DimensionTableDataManager.createInstanceByTableName(tableName);
    _tableDataManager.init(instanceDataManagerConfig, helixManager, new SegmentLocks(), tableConfig, null, null,
        SEGMENT_OPERATIONS_THROTTLER);
    _tableDataManager.start();

    for (int i = 0; i < _indexSegments.size(); i++) {
      _tableDataManager.addSegment((ImmutableSegment) _indexSegments.get(i));
    }

    return _tableDataManager;
  }

  @TearDown(Level.Iteration)
  public void tearDown() {
    _tableDataManager.shutDown();
    _tableDataManager.releaseDimensionTable();

    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }

    FileUtils.deleteQuietly(INDEX_DIR);
    EXECUTOR_SERVICE.shutdownNow();

    _iteration++;
  }

  private TableConfig getTableConfig(boolean disablePreload) {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setFieldConfigList(FIELD_CONFIGS)
        .setNoDictionaryColumns(List.of(RAW_INT_COL_NAME, RAW_STRING_COL_NAME, TIMESTAMP_COL))
        .setSortedColumn(SORTED_COL_NAME)
        .setDimensionTableConfig(new DimensionTableConfig(disablePreload, false))
        .build();
  }

  static LazyDataGenerator createTestData(int numRows, Distribution.DataSupplier supplier) {
    //create data lazily to prevent OOM and speed up setup

    return new LazyDataGenerator() {
      private final Map<Integer, UUID> _strings = new HashMap<>();
      private final String[] _lowCardinalityValues =
          IntStream.range(0, 10).mapToObj(i -> "value" + i).toArray(String[]::new);
      private Distribution.DataSupplier _supplier = supplier;

      @Override
      public int size() {
        return numRows;
      }

      @Override
      public GenericRow next(GenericRow row, int i) {
        row.putValue(SORTED_COL_NAME, numRows - i);
        row.putValue(INT_COL_NAME, (int) _supplier.getAsLong());
        row.putValue(NO_INDEX_INT_COL_NAME, (int) _supplier.getAsLong());
        row.putValue(RAW_INT_COL_NAME, (int) _supplier.getAsLong());
        long rawStrKey = (_supplier.getAsLong() % 100000);
        row.putValue(RAW_STRING_COL_NAME,
            _strings.computeIfAbsent((int) rawStrKey, k -> UUID.randomUUID()).toString());
        row.putValue(NO_INDEX_STRING_COL, row.getValue(RAW_STRING_COL_NAME));
        row.putValue(LOW_CARDINALITY_STRING_COL, _lowCardinalityValues[i % _lowCardinalityValues.length]);
        row.putValue(TIMESTAMP_COL, i * 1200 * 1000L);

        return null;
      }

      @Override
      public void rewind() {
        _strings.clear();
        _supplier.reset();
      }
    };
  }

  private void buildSegment(String segmentName, TableConfig tableConfig)
      throws Exception {

    LazyDataGenerator dataGenerator = createTestData(_numRows, _supplier);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GeneratedDataRecordReader(dataGenerator)) {
      driver.init(config, recordReader);
      driver.build();
    }

    //save generator state so that other segments are not identical to this one
    _supplier.snapshot();
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
