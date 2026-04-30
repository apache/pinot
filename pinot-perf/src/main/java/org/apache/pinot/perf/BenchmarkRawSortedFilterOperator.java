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

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.ScanBasedFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
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


/**
 * JMH benchmark comparing the raw sorted filter path against the scan fallback on a real immutable segment.
 *
 * <p>The benchmark builds a raw, single-value, sorted integer column and measures the end-to-end leaf filter cost for
 * two cases:
 * {@link org.apache.pinot.core.operator.filter.RawSortedIndexBasedFilterOperator} via the default planner path, and
 * {@link ScanBasedFilterOperator} by forcing the query context to skip the sorted index.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkRawSortedFilterOperator {
  private static final File INDEX_DIR_ROOT =
      new File(FileUtils.getTempDirectory(), "BenchmarkRawSortedFilterOperator");
  private static final String TABLE_NAME = "rawSortedFilterTable";
  private static final String COLUMN_NAME = "rawSortedInt";
  private static final String RAW_SORTED_OPERATOR_NAME = "RawSortedIndexBasedFilterOperator";
  private static final ExpressionContext COLUMN_EXPRESSION = ExpressionContext.forIdentifier(COLUMN_NAME);
  private static final int RUN_LENGTH = 32;

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkRawSortedFilterOperator.class.getSimpleName()).build()).run();
  }

  @State(Scope.Benchmark)
  public static class FilterState {
    @Param({"1000000"})
    int _numRows;

    @Param({"PASS_THROUGH", "SNAPPY"})
    String _compressionCodec;

    @Param({"EQ", "RANGE_1_PERCENT", "RANGE_10_PERCENT"})
    String _predicateShape;

    private File _indexDir;
    private Schema _schema;
    private IndexSegment _segment;
    private DataSource _dataSource;
    private QueryContext _rawSortedQueryContext;
    private QueryContext _scanQueryContext;
    private PredicateEvaluator _predicateEvaluator;
    private long _expectedChecksum;

    @Setup(Level.Trial)
    public void setup()
        throws Exception {
      _indexDir = new File(INDEX_DIR_ROOT, _compressionCodec + "-" + _predicateShape + "-" + _numRows);
      FileUtils.deleteQuietly(_indexDir);
      FileUtils.forceMkdir(_indexDir);

      _schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
          .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
          .build();

      buildSegment();
      loadSegment();

      Preconditions.checkState(_dataSource.getDataSourceMetadata().isSorted(),
          "Benchmark requires a sorted column but metadata was unsorted");
      Preconditions.checkState(_dataSource.getDictionary() == null,
          "Benchmark requires a raw column but dictionary was present");
      _rawSortedQueryContext = new QueryContext.Builder().setTableName(TABLE_NAME).build();
      _rawSortedQueryContext.setSchema(_schema);

      _scanQueryContext = new QueryContext.Builder().setTableName(TABLE_NAME).build();
      _scanQueryContext.setSchema(_schema);
      _scanQueryContext.setSkipIndexes(Collections.singletonMap(COLUMN_NAME, EnumSet.of(FieldConfig.IndexType.SORTED)));

      Predicate predicate = buildPredicate();
      _predicateEvaluator = PredicateEvaluatorProvider.getPredicateEvaluator(predicate, _dataSource,
          _rawSortedQueryContext);

      BaseFilterOperator rawSortedOperator = createRawSortedOperator();
      Preconditions.checkState(RAW_SORTED_OPERATOR_NAME.equals(rawSortedOperator.getClass().getSimpleName()),
          "Expected %s but planned %s", RAW_SORTED_OPERATOR_NAME, rawSortedOperator.getClass().getName());
      BaseFilterOperator scanOperator = createScanOperator();
      Preconditions.checkState(scanOperator instanceof ScanBasedFilterOperator,
          "Expected ScanBasedFilterOperator but planned %s", scanOperator.getClass().getName());

      _expectedChecksum = consume(rawSortedOperator);
      long scanChecksum = consume(scanOperator);
      Preconditions.checkState(_expectedChecksum == scanChecksum,
          "Raw sorted and scan paths must return identical results. rawSorted=%s scan=%s shape=%s",
          _expectedChecksum, scanChecksum, _predicateShape);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      if (_segment != null) {
        _segment.destroy();
      }
      FileUtils.deleteQuietly(_indexDir);
    }

    BaseFilterOperator createRawSortedOperator() {
      return FilterOperatorUtils.getLeafFilterOperator(_rawSortedQueryContext, _predicateEvaluator, _dataSource,
          _numRows);
    }

    BaseFilterOperator createScanOperator() {
      return FilterOperatorUtils.getLeafFilterOperator(_scanQueryContext, _predicateEvaluator, _dataSource, _numRows);
    }

    private void buildSegment()
        throws Exception {
      FieldConfig fieldConfig = new FieldConfig.Builder(COLUMN_NAME)
          .withEncodingType(FieldConfig.EncodingType.RAW)
          .withCompressionCodec(FieldConfig.CompressionCodec.valueOf(_compressionCodec))
          .build();
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(
          new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
              .setNoDictionaryColumns(List.of(COLUMN_NAME))
              .setSortedColumn(COLUMN_NAME)
              .setFieldConfigList(List.of(fieldConfig))
              .build(),
          _schema);
      config.setOutDir(_indexDir.getAbsolutePath());
      config.setTableName(TABLE_NAME);
      config.setSegmentName("segment");

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      try (RecordReader recordReader = new GeneratedDataRecordReader(createDataGenerator())) {
        driver.init(config, recordReader);
        driver.build();
      }
    }

    private void loadSegment()
        throws Exception {
      _segment = ImmutableSegmentLoader.load(new File(_indexDir, "segment"),
          new IndexLoadingConfig(
              new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
                  .setNoDictionaryColumns(List.of(COLUMN_NAME))
                  .setSortedColumn(COLUMN_NAME)
                  .setFieldConfigList(List.of(new FieldConfig.Builder(COLUMN_NAME)
                      .withEncodingType(FieldConfig.EncodingType.RAW)
                      .withCompressionCodec(FieldConfig.CompressionCodec.valueOf(_compressionCodec))
                      .build()))
                  .build(),
              _schema));
      _dataSource = _segment.getDataSource(COLUMN_NAME);
    }

    private Predicate buildPredicate() {
      int numDistinctValues = (_numRows + RUN_LENGTH - 1) / RUN_LENGTH;
      int midValue = numDistinctValues / 2;
      switch (_predicateShape) {
        case "EQ":
          return new EqPredicate(COLUMN_EXPRESSION, Integer.toString(midValue));
        case "RANGE_1_PERCENT":
          return createCenteredRangePredicate(numDistinctValues, 0.01d);
        case "RANGE_10_PERCENT":
          return createCenteredRangePredicate(numDistinctValues, 0.10d);
        default:
          throw new IllegalStateException("Unsupported predicate shape: " + _predicateShape);
      }
    }

    private Predicate createCenteredRangePredicate(int numDistinctValues, double matchingFraction) {
      int matchingDocs = Math.max(1, (int) Math.round(_numRows * matchingFraction));
      int matchingDistinctValues = Math.max(1, (matchingDocs + RUN_LENGTH - 1) / RUN_LENGTH);
      int lowerValue = Math.max(0, (numDistinctValues - matchingDistinctValues) / 2);
      int upperValue = Math.min(numDistinctValues - 1, lowerValue + matchingDistinctValues - 1);
      return new RangePredicate(COLUMN_EXPRESSION, true, Integer.toString(lowerValue), true,
          Integer.toString(upperValue), FieldSpec.DataType.INT);
    }

    private LazyDataGenerator createDataGenerator() {
      return new LazyDataGenerator() {
        @Override
        public int size() {
          return _numRows;
        }

        @Override
        public GenericRow next(GenericRow row, int index) {
          row.putValue(COLUMN_NAME, index / RUN_LENGTH);
          return row;
        }

        @Override
        public void rewind() {
        }
      };
    }
  }

  @Benchmark
  public long rawSortedPath(FilterState state) {
    return consume(state.createRawSortedOperator());
  }

  @Benchmark
  public long scanPath(FilterState state) {
    return consume(state.createScanOperator());
  }

  private static long consume(BaseFilterOperator operator) {
    FilterBlock filterBlock = operator.nextBlock();
    long checksum = 1L;
    int numMatches = 0;
    int docId;
    var iterator = filterBlock.getBlockDocIdSet().iterator();
    while ((docId = iterator.next()) != Constants.EOF) {
      checksum = checksum * 31 + docId;
      numMatches++;
    }
    return checksum ^ ((long) numMatches << 32);
  }
}
