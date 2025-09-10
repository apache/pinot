/**
 * Licensed to the Apache Softwa' Foundation (ASF) under one
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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.queries.BaseQueriesTest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.tools.SortedColumnQuickstart;
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
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkOrderByQueries extends BaseQueriesTest {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(BenchmarkOrderByQueries.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FilteredAggregationsTest");
  private static final String FIRST_SEGMENT_NAME = "firstTestSegment";
  private static final String SECOND_SEGMENT_NAME = "secondTestSegment";

  @Param({"ASC", "NAIVE_DESC", "REVERSE_ITERATOR", "REVERSE_OPERATOR"})
  private Mode _zMode; // called zMode just to force this parameter to be the last used in the report
  @Param("1500000")
  private int _numRows;
  //@Param({"EXP(0.5)"})
  String _scenario = "EXP(0.5)";
  @Param({"1", "1000"})
  int _primaryRepetitions;
  @Param({"1500", "150000"})
  int _limit;
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private LongSupplier _supplier;

  @Setup
  public void setUp()
      throws Exception {
    if (_zMode == Mode.REVERSE_ITERATOR) {
      System.setProperty("USE_REVERSE_ITERATOR", "true");
    }

    _supplier = Distribution.createSupplier(42, _scenario);
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment(FIRST_SEGMENT_NAME);
    buildSegment(SECOND_SEGMENT_NAME);

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(
        SortedColumnQuickstart.SortedTable.TABLE_CONFIG,
        SortedColumnQuickstart.SortedTable.SCHEMA
    );

    ImmutableSegment firstImmutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, FIRST_SEGMENT_NAME), indexLoadingConfig);
    ImmutableSegment secondImmutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SECOND_SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = firstImmutableSegment;
    _indexSegments = Arrays.asList(firstImmutableSegment, secondImmutableSegment);
  }

  @TearDown
  public void tearDown() {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }

    FileUtils.deleteQuietly(INDEX_DIR);
    EXECUTOR_SERVICE.shutdownNow();
  }

  private void buildSegment(String segmentName)
      throws Exception {
    List<GenericRow> rows = SortedColumnQuickstart.SortedTable.streamData(_primaryRepetitions, _supplier)
        .limit(_numRows)
        .collect(Collectors.toCollection(() -> new ArrayList<>(_numRows)));
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(
        SortedColumnQuickstart.SortedTable.TABLE_CONFIG,
        SortedColumnQuickstart.SortedTable.SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(SortedColumnQuickstart.SortedTable.SCHEMA.getSchemaName());
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Benchmark
  public BrokerResponseNative sortedTotally() {
    switch (_zMode) {
      case ASC:
        return getBrokerResponse(
            "SELECT SORTED_COL "
                + "FROM sorted "
                + "ORDER BY SORTED_COL ASC "
                + "LIMIT " + _limit);
      case NAIVE_DESC:
        return getBrokerResponse(
            "SET allowReverseOrder=false;"
                + "SELECT SORTED_COL "
                + "FROM sorted "
                + "ORDER BY SORTED_COL DESC "
                + "LIMIT " + _limit);
      case REVERSE_ITERATOR:
      case REVERSE_OPERATOR:
        return getBrokerResponse(
            "SET allowReverseOrder=true;"
                + "SELECT SORTED_COL "
                + "FROM sorted "
                + "ORDER BY SORTED_COL DESC "
                + "LIMIT " + _limit);
      default:
        throw new IllegalStateException("Unknown mode: " + _zMode);
    }
  }

  @Benchmark
  public BrokerResponseNative sortedPartially() {
    switch (_zMode) {
      case ASC:
        return getBrokerResponse(
            "SELECT SORTED_COL "
                + "FROM sorted "
                + "ORDER BY SORTED_COL ASC, LOW_CARDINALITY_STRING_COL "
                + "LIMIT " + _limit);
      case NAIVE_DESC:
        return getBrokerResponse(
            "SET allowReverseOrder=false;"
                + "SELECT SORTED_COL "
                + "FROM sorted "
                + "ORDER BY SORTED_COL DESC, LOW_CARDINALITY_STRING_COL "
                + "LIMIT " + _limit);
      case REVERSE_ITERATOR:
      case REVERSE_OPERATOR:
        return getBrokerResponse(
            "SET allowReverseOrder=true;"
                + "SELECT SORTED_COL "
                + "FROM sorted "
                + "ORDER BY SORTED_COL DESC, LOW_CARDINALITY_STRING_COL "
                + "LIMIT " + _limit);
      default:
        throw new IllegalStateException("Unknown mode: " + _zMode);
    }
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

  public enum Mode {
    ASC, NAIVE_DESC, REVERSE_ITERATOR, REVERSE_OPERATOR
  }
}
