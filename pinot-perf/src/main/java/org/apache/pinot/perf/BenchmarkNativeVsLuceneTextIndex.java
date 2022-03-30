package org.apache.pinot.perf;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkNativeVsLuceneTextIndex {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkNativeAndLuceneBasedLike");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String DOMAIN_NAMES_COL_LUCENE = "DOMAIN_NAMES_LUCENE";
  private static final String DOMAIN_NAMES_COL_NATIVE = "DOMAIN_NAMES_NATIVE";
  private static final String URL_COL = "URL_COL";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String NO_INDEX_STRING_COL_NAME = "NO_INDEX_COL";

  @Param({"SELECT * FROM MyTable WHERE TEXT_MATCH(DOMAIN_NAMES_LUCENE, 'www.domain1%') LIMIT 5000000",
      "SELECT * FROM MyTable WHERE TEXT_MATCH(DOMAIN_NAMES_NATIVE, 'www.domain1%') LIMIT 5000000"})
  String _query;
  @Param("250000")
  int _numRows;
  @Param("1000")
  int _intBaseValue;
  @Param({"0", "1", "10", "100"})
  int _numBlocks;

  private PlanMaker _planMaker;
  private IndexSegment _indexSegment;
  private QueryContext _queryContext;

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    _planMaker = new InstancePlanMakerImplV2();
    _queryContext = QueryContextConverterUtils.getQueryContextFromSQL(_query);
    FileUtils.deleteQuietly(INDEX_DIR);
    buildSegment();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Set<String> textIndexCols = new HashSet<>();
    textIndexCols.add(DOMAIN_NAMES_COL_LUCENE);
    textIndexCols.add(DOMAIN_NAMES_COL_NATIVE);
    indexLoadingConfig.setTextIndexColumns(textIndexCols);
    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private List<GenericRow> createTestData(int numRows) {
    List<GenericRow> rows = new ArrayList<>(numRows);
    String[] domainNames = new String[]{
        "www.domain1.com", "www.domain1.co.ab", "www.domain1.co.bc",
        "www.domain1.co.cd", "www.sd.domain1.com", "www.sd.domain1.co.ab", "www.sd.domain1.co.bc",
        "www.sd.domain1.co.cd", "www.domain2.com", "www.domain2.co.ab", "www.domain2.co.bc", "www.domain2.co.cd",
        "www.sd.domain2.com", "www.sd.domain2.co.ab", "www.sd.domain2.co.bc", "www.sd.domain2.co.cd"
    };
    String[] urlSuffixes = new String[] {"/a", "/b", "/c", "/d"};
    String[] noIndexData = new String[]{"test1", "test2", "test3", "test4", "test5"};
    for (int i = 0; i < numRows; i++) {
      String domain = domainNames[i % domainNames.length];
      String url = domain + urlSuffixes[i % urlSuffixes.length];
      GenericRow row = new GenericRow();
      row.putField(INT_COL_NAME, _intBaseValue + i);
      row.putField(NO_INDEX_STRING_COL_NAME, noIndexData[i % noIndexData.length]);
      row.putField(DOMAIN_NAMES_COL_LUCENE, domain);
      row.putField(DOMAIN_NAMES_COL_NATIVE, domain);
      row.putField(URL_COL, url);
      rows.add(row);
    }
    return rows;
  }

  private void buildSegment()
      throws Exception {
    List<GenericRow> rows = createTestData(_numRows);
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    Map<String, String> propertiesMap = new HashMap<>();
    propertiesMap.put(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL);

    fieldConfigs.add(
        new FieldConfig(DOMAIN_NAMES_COL_LUCENE, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null, null));
    fieldConfigs.add(
        new FieldConfig(DOMAIN_NAMES_COL_NATIVE, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null, propertiesMap));
    fieldConfigs
        .add(new FieldConfig(URL_COL, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null, null));
    List<String> invertedIndexColumns = new ArrayList<>();

    invertedIndexColumns.add(DOMAIN_NAMES_COL_NATIVE);
    invertedIndexColumns.add(DOMAIN_NAMES_COL_LUCENE);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(invertedIndexColumns).setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DOMAIN_NAMES_COL_LUCENE, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DOMAIN_NAMES_COL_NATIVE, FieldSpec.DataType.STRING)
        .addSingleValueDimension(URL_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(NO_INDEX_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addMetric(INT_COL_NAME, FieldSpec.DataType.INT).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void query(Blackhole bh) {
    Operator<?> operator = _planMaker.makeSegmentPlanNode(_indexSegment, _queryContext).run();
    bh.consume(operator);
    for (int i = 0; i < _numBlocks; i++) {
      bh.consume(operator.nextBlock());
    }
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkNativeVsLuceneTextIndex.class
        .getSimpleName()).build()).run();
  }
}
