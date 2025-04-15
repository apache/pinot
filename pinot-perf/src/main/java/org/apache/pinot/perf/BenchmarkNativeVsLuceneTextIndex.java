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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
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


@BenchmarkMode(Mode.Throughput)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.MINUTES)
@State(Scope.Benchmark)
public class BenchmarkNativeVsLuceneTextIndex {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TextSearchQueriesTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME_NATIVE = "testSegmentNative";
  private static final String DOMAIN_NAMES_COL = "DOMAIN_NAMES_COL";
  private static final String INT_COL = "INT_COL";
  private static final String NATIVE_QUERY = "SELECT SUM(INT_COL) FROM MyTable "
      + "WHERE TEXT_CONTAINS(DOMAIN_NAMES_COL, 'sac.*') OR TEXT_CONTAINS(DOMAIN_NAMES_COL, 'vic.*')";
  private static final String LUCENE_QUERY =
      "SELECT SUM(INT_COL) FROM MyTable WHERE TEXT_MATCH(DOMAIN_NAMES_COL, 'sac* OR vic*')";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension(DOMAIN_NAMES_COL, FieldSpec.DataType.STRING)
      .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT).build();

  private IndexSegment _indexSegment;

  @Param("1000000")
  int _numRows;
  @Param({"0", "1", "10", "100"})
  int _numBlocks;
  @Param({"NATIVE", "LUCENE"})
  private FSTType _fstType;

  private PlanMaker _planMaker;
  private QueryContext _queryContext;

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    _planMaker = new InstancePlanMakerImplV2();
    if (_fstType == FSTType.LUCENE) {
      _queryContext = QueryContextConverterUtils.getQueryContext(LUCENE_QUERY);
    } else {
      _queryContext = QueryContextConverterUtils.getQueryContext(NATIVE_QUERY);
    }
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment(_fstType);

    _indexSegment = loadSegment(_fstType);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private List<String> getDomainNames() {
    return Arrays.asList("Prince Andrew kept looking with an amused smile from Pierre",
        "vicomte and from the vicomte to their hostess. In the first moment of",
        "Pierre’s outburst Anna Pávlovna, despite her social experience, was",
        "horror-struck. But when she saw that Pierre’s sacrilegious words",
        "had not exasperated the vicomte, and had convinced herself that it was",
        "impossible to stop him, she rallied her forces and joined the vicomte in", "a vigorous attack on the orator",
        "horror-struck. But when she", "she rallied her forces and joined", "outburst Anna Pávlovna",
        "she rallied her forces and", "despite her social experience", "had not exasperated the vicomte",
        " despite her social experience", "impossible to stop him", "despite her social experience");
  }

  private List<GenericRow> createTestData(int numRows) {
    List<GenericRow> rows = new ArrayList<>();
    List<String> domainNames = getDomainNames();
    for (int i = 0; i < numRows; i++) {
      String domain = domainNames.get(i % domainNames.size());
      GenericRow row = new GenericRow();
      row.putField(DOMAIN_NAMES_COL, domain);
      row.putField(INT_COL, i);
      rows.add(row);
    }
    return rows;
  }

  private void buildSegment(FSTType fstType)
      throws Exception {
    List<GenericRow> rows = createTestData(_numRows);
    TableConfig tableConfig = getTableConfig(fstType);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME_NATIVE);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private ImmutableSegment loadSegment(FSTType fstType)
      throws Exception {
    TableConfig tableConfig = getTableConfig(fstType);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_NATIVE), indexLoadingConfig);
  }

  private static TableConfig getTableConfig(FSTType fstType) {
    Map<String, String> propertiesMap = new HashMap<>();
    if (fstType == FSTType.NATIVE) {
      propertiesMap.put(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL);
    }
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig(DOMAIN_NAMES_COL, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null,
            fstType == FSTType.NATIVE ? propertiesMap : null));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(DOMAIN_NAMES_COL)).setFieldConfigList(fieldConfigs).build();
    tableConfig.getIndexingConfig().setFSTIndexType(fstType);
    return tableConfig;
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void query(Blackhole bh) {
    Operator<?> operator = _planMaker.makeSegmentPlanNode(new SegmentContext(_indexSegment), _queryContext).run();
    for (int i = 0; i < _numBlocks; i++) {
      bh.consume(operator.nextBlock());
    }
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkNativeVsLuceneTextIndex.class.getSimpleName()).build()).run();
  }
}
