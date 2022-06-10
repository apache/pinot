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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.queries.BaseQueriesTest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FSTType;
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
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkFuseRegexp extends BaseQueriesTest {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkFuseRegexp");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String DOMAIN_NAMES_COL = "DOMAIN_NAMES";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String NO_INDEX_STRING_COL_NAME = "NO_INDEX_COL";

  @Param({"LUCENE", "NATIVE", "null"})
  private String _fstType;
  //@Param({"DOMAIN_NAMES LIKE '%domain<i>%'"})
  String _predicate = "DOMAIN_NAMES LIKE '%domain<i>%'";
  //@Param({"and", "or"})
  String _conjunction = "or";
//  @Param("2500000")
  int _numRows = 2500000;
//  @Param("1000")
  int _intBaseValue = 1000;
//  @Param({"100"})
  int _numBlocks = 100;

  private IndexSegment _indexSegment;
  private Schema _schema;
  private TableConfig _tableConfig;

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    FSTType fstType = _fstType.equals("null") ? null : FSTType.valueOf(_fstType);
    buildSegment(fstType);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    if (fstType != null) {
      Set<String> fstIndexCols = new HashSet<>();
      fstIndexCols.add(DOMAIN_NAMES_COL);
      indexLoadingConfig.setFSTIndexColumns(fstIndexCols);
      indexLoadingConfig.setFSTIndexType(fstType);
    }
    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);

    checkCorrectness();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
    EXECUTOR_SERVICE.shutdownNow();
  }

  private List<GenericRow> createTestData(int numRows) {
    List<GenericRow> rows = new ArrayList<>(numRows);
    String[] domainNames = new String[]{
        "www.domain1.com", "www.domain1.co.ab", "www.domain1.co.bc",
        "www.domain1.co.cd", "www.sd.domain1.com", "www.sd.domain1.co.ab", "www.sd.domain1.co.bc",
        "www.sd.domain1.co.cd", "www.domain2.com", "www.domain2.co.ab", "www.domain2.co.bc", "www.domain2.co.cd",
        "www.sd.domain2.com", "www.sd.domain2.co.ab", "www.sd.domain2.co.bc", "www.sd.domain2.co.cd"
    };
    String[] noIndexData = new String[]{"test1", "test2", "test3", "test4", "test5"};
    for (int i = 0; i < numRows; i++) {
      String domain = domainNames[i % domainNames.length];
      GenericRow row = new GenericRow();
      row.putField(INT_COL_NAME, _intBaseValue + i);
      row.putField(NO_INDEX_STRING_COL_NAME, noIndexData[i % noIndexData.length]);
      row.putField(DOMAIN_NAMES_COL, domain);
      rows.add(row);
    }
    return rows;
  }

  private void buildSegment(FSTType fstType)
      throws Exception {
    List<GenericRow> rows = createTestData(_numRows);
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    fieldConfigs.add(
        new FieldConfig(DOMAIN_NAMES_COL, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null, null));
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Collections.singletonList(DOMAIN_NAMES_COL)).setFieldConfigList(fieldConfigs).build();
    _schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DOMAIN_NAMES_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(NO_INDEX_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addMetric(INT_COL_NAME, FieldSpec.DataType.INT).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    if (fstType != null) {
      config.setFSTIndexType(fstType);
    }
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void optimal1Like(Blackhole bh) {
    String query = "SELECT INT_COL FROM MyTable WHERE DOMAIN_NAMES LIKE '%domain0%'";
    bh.consume(getBrokerResponse(query));
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void optimal1Regex(Blackhole bh) {
    String query = "SELECT INT_COL FROM MyTable WHERE regexp_like(DOMAIN_NAMES, 'domain0')";
    bh.consume(getBrokerResponse(query));
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void optimal10(Blackhole bh) {
    String query = "SELECT INT_COL FROM MyTable WHERE regexp_like(DOMAIN_NAMES, 'domain\\d')";
    bh.consume(getBrokerResponse(query));
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void increasing10Like(Blackhole bh) {
    String query = "SELECT INT_COL FROM MyTable WHERE "
        + "DOMAIN_NAMES LIKE '%domain0%' or "
        + "DOMAIN_NAMES LIKE '%domain1%' or "
        + "DOMAIN_NAMES LIKE '%domain2%' or "
        + "DOMAIN_NAMES LIKE '%domain3%' or "
        + "DOMAIN_NAMES LIKE '%domain4%' or "
        + "DOMAIN_NAMES LIKE '%domain5%' or "
        + "DOMAIN_NAMES LIKE '%domain6%' or "
        + "DOMAIN_NAMES LIKE '%domain7%' or "
        + "DOMAIN_NAMES LIKE '%domain8%' or "
        + "DOMAIN_NAMES LIKE '%domain9%'";
    bh.consume(getBrokerResponse(query));
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void increasing10Regex(Blackhole bh) {
    String query = "SELECT INT_COL FROM MyTable WHERE "
        + "regexp_like(DOMAIN_NAMES, '^.domain0.*$') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain1.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain2.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain3.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain4.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain5.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain6.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain7.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain8.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain9.*')";
    bh.consume(getBrokerResponse(query));
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void increasing10Fusing(Blackhole bh) {
    String query = "SELECT INT_COL FROM MyTable WHERE "
        + "regexp_like(DOMAIN_NAMES, '"
        + "(?:^.*domain0.*$)|"
        + "(?:^.*domain1.*$)|"
        + "(?:^.*domain2.*$)|"
        + "(?:^.*domain3.*$)|"
        + "(?:^.*domain4.*$)|"
        + "(?:^.*domain5.*$)|"
        + "(?:^.*domain6.*$)|"
        + "(?:^.*domain7.*$)|"
        + "(?:^.*domain8.*$)|"
        + "(?:^.*domain9.*$)')";

    bh.consume(getBrokerResponse(query));
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void decreasing9Like(Blackhole bh) {
    String query = "SELECT INT_COL FROM MyTable WHERE "
        + "DOMAIN_NAMES LIKE '%domain9%' or "
        + "DOMAIN_NAMES LIKE '%domain8%' or "
        + "DOMAIN_NAMES LIKE '%domain7%' or "
        + "DOMAIN_NAMES LIKE '%domain6%' or "
        + "DOMAIN_NAMES LIKE '%domain5%' or "
        + "DOMAIN_NAMES LIKE '%domain4%' or "
        + "DOMAIN_NAMES LIKE '%domain3%' or "
        + "DOMAIN_NAMES LIKE '%domain2%' or "
        + "DOMAIN_NAMES LIKE '%domain1%'";
    bh.consume(getBrokerResponse(query));
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void decreasing9Regex(Blackhole bh) {
    String query = "SELECT INT_COL FROM MyTable WHERE "
        + "regexp_like(DOMAIN_NAMES, '^.domain9.*$') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain8.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain7.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain6.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain5.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain4.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain3.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain2.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain2.*')";
    bh.consume(getBrokerResponse(query));
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void decreasing9Fusing(Blackhole bh) {
    String query = "SELECT INT_COL FROM MyTable WHERE "
        + "regexp_like(DOMAIN_NAMES, '"
        + "(?:^.*domain9.*$)|"
        + "(?:^.*domain8.*$)|"
        + "(?:^.*domain7.*$)|"
        + "(?:^.*domain6.*$)|"
        + "(?:^.*domain5.*$)|"
        + "(?:^.*domain4.*$)|"
        + "(?:^.*domain3.*$)|"
        + "(?:^.*domain2.*$)|"
        + "(?:^.*domain1.*$)')";

    bh.consume(getBrokerResponse(query));
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
    return Collections.singletonList(_indexSegment);
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkFuseRegexp.class.getSimpleName())
//        .addProfiler(JavaFlightRecorderProfiler.class)
        .build()).run();
  }

  private void checkCorrectness() {
    String query1 = "SELECT INT_COL FROM MyTable WHERE "
        + "regexp_like(DOMAIN_NAMES, '^.domain9.*$') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain8.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain7.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain6.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain5.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain4.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain3.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain2.*') or "
        + "regexp_like(DOMAIN_NAMES, '^.domain2.*')";
    String query2 = "SELECT INT_COL FROM MyTable WHERE "
        + "regexp_like(DOMAIN_NAMES, '"
        + "(?:^.*domain9.*$)|"
        + "(?:^.*domain8.*$)|"
        + "(?:^.*domain7.*$)|"
        + "(?:^.*domain6.*$)|"
        + "(?:^.*domain5.*$)|"
        + "(?:^.*domain4.*$)|"
        + "(?:^.*domain3.*$)|"
        + "(?:^.*domain2.*$)|"
        + "(?:^.*domain1.*$)')";

    long total1 = getBrokerResponse(query1).getTotalDocs();
    long total2 = getBrokerResponse(query2).getTotalDocs();

    if (total1 != total2) {
      throw new RuntimeException("Expressions are not equivalent: " + total1 + " vs " + total2);
    }
  }
}
