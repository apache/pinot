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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.queries.BaseQueriesTest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
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
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testng.annotations.AfterClass;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class BenchmarkNativeAndLuceneBasedLike extends BaseQueriesTest {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkNativeAndLuceneBasedLike");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String DOMAIN_NAMES_COL = "DOMAIN_NAMES";
  private static final String URL_COL = "URL_COL";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String NO_INDEX_STRING_COL_NAME = "NO_INDEX_COL";
  private static final Integer INT_BASE_VALUE = 1000;
  private static final Integer NUM_ROWS = 2500000;

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private IndexSegment _luceneBasedFSTIndexSegment;
  private IndexSegment _nativeBasedFSTIndexSegment;

  String _query = "SELECT INT_COL, URL_COL FROM MyTable "
      + "WHERE DOMAIN_NAMES LIKE '%domain%'";

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @Setup
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    List<IndexSegment> segments = new ArrayList<>();
    for (FSTType fstType : Arrays.asList(FSTType.LUCENE, FSTType.NATIVE)) {
      buildSegment(fstType);

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
      Set<String> fstIndexCols = new HashSet<>();
      fstIndexCols.add(DOMAIN_NAMES_COL);
      indexLoadingConfig.setFSTIndexColumns(fstIndexCols);
      indexLoadingConfig.setFSTIndexType(fstType);

      ImmutableSegment segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);

      segments.add(segment);

      if (fstType == FSTType.LUCENE) {
        _luceneBasedFSTIndexSegment = segment;
      } else {
        _nativeBasedFSTIndexSegment = segment;
      }
    }

    _indexSegment = segments.get(ThreadLocalRandom.current().nextInt(2));
    _indexSegments = segments;
  }

  @AfterClass
  public void tearDown() {
    _nativeBasedFSTIndexSegment.destroy();
    _luceneBasedFSTIndexSegment.destroy();

    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private List<GenericRow> createTestData(int numRows) {
    List<GenericRow> rows = new ArrayList<>();

    List<String> domainNames = getDomainNames();
    List<String> urlSufficies = getURLSufficies();
    List<String> noIndexData = getNoIndexData();

    for (int i = 0; i < numRows; i++) {
      String domain = domainNames.get(i % domainNames.size());
      String url = domain + urlSufficies.get(i % urlSufficies.size());

      GenericRow row = new GenericRow();
      row.putField(INT_COL_NAME, INT_BASE_VALUE + i);
      row.putField(NO_INDEX_STRING_COL_NAME, noIndexData.get(i % noIndexData.size()));
      row.putField(DOMAIN_NAMES_COL, domain);
      row.putField(URL_COL, url);
      rows.add(row);
    }
    return rows;
  }

  private void buildSegment(FSTType fstType)
      throws Exception {
    List<GenericRow> rows = createTestData(NUM_ROWS);
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    fieldConfigs.add(
        new FieldConfig(DOMAIN_NAMES_COL, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null, null));
    fieldConfigs
        .add(new FieldConfig(URL_COL, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null, null));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList(DOMAIN_NAMES_COL)).setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DOMAIN_NAMES_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(URL_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(NO_INDEX_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addMetric(INT_COL_NAME, FieldSpec.DataType.INT).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setFSTIndexType(fstType);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private List<String> getURLSufficies() {
    return Arrays.asList("/a", "/b", "/c", "/d");
  }

  private List<String> getNoIndexData() {
    return Arrays.asList("test1", "test2", "test3", "test4", "test5");
  }

  private List<String> getDomainNames() {
    return Arrays
        .asList("www.domain1.com", "www.domain1.co.ab", "www.domain1.co.bc", "www.domain1.co.cd", "www.sd.domain1.com",
            "www.sd.domain1.co.ab", "www.sd.domain1.co.bc", "www.sd.domain1.co.cd", "www.domain2.com",
            "www.domain2.co.ab", "www.domain2.co.bc", "www.domain2.co.cd", "www.sd.domain2.com", "www.sd.domain2.co.ab",
            "www.sd.domain2.co.bc", "www.sd.domain2.co.cd");
  }

  private void executeTest(String query, Blackhole blackhole) {
    Operator<IntermediateResultsBlock> operator = getOperatorForSqlQuery(query);

    blackhole.consume(operator.nextBlock());
  }

  @Benchmark
  public void testLuceneBasedFSTLike(Blackhole blackhole) {
    _indexSegments = Arrays.asList(_luceneBasedFSTIndexSegment);

    for (int i = 0; i < 10000; i++) {
      executeTest(_query, blackhole);
    }
  }

  @Benchmark
  public void testNativeBasedFSTLike(Blackhole blackhole) {
    _indexSegments = Arrays.asList(_nativeBasedFSTIndexSegment);

    for (int i = 0; i < 10000; i++) {
      executeTest(_query, blackhole);
    }
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkNativeAndLuceneBasedLike.class
        .getSimpleName()).build()).run();
  }
}
