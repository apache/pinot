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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentpruner.TimeSegmentPruner;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Benchmarks the broker-side segment metadata maintenance path of [TimeSegmentPruner].
///
/// Every LLC REALTIME segment commit makes the controller broadcast a segment refresh message to every broker, so on a
/// large table the per-broker refresh rate is a multiple of the per-broker query rate. `refreshBurstThenPrune`
/// models that ratio and is the number to compare across changes; `refreshChangedInterval` and
/// `refreshUnchangedInterval` isolate the two refresh shapes (a committing REALTIME segment getting its time range for
/// the first time, and an OFFLINE segment re-pushed with the same time range). `prune` and `pruneAfterSegmentChange`
/// bracket the query path: the cost when the interval tree is already built, and the cost when the query is the one
/// that has to build it.
///
/// Run with `-prof gc` — allocation rate matters more than latency here, because the garbage this path produces is
/// promoted to old gen rather than dying in eden.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@State(Scope.Benchmark)
public class BenchmarkTimeSegmentPruner {
  private static final String RAW_TABLE_NAME = "benchTable";
  private static final String TIME_COLUMN = "tsMs";
  private static final long BASE_TIME_MS = 1704067200000L; // 2024-01-01T00:00:00Z
  private static final long SEGMENT_SPAN_MS = TimeUnit.HOURS.toMillis(1);
  /// Refresh messages are broadcast to every broker while queries are spread across them, so each broker sees several
  /// refreshes per query it serves.
  private static final int REFRESHES_PER_QUERY = 6;

  @Param({"10000", "100000", "250000"})
  private int _numSegments;

  /// Number of segments sharing one `[start, end]` interval. 1 models REALTIME segments whose time range comes from
  /// the data (effectively all distinct); 64 models one segment per stream partition landing on the same coarse
  /// time bucket.
  @Param({"1", "64"})
  private int _numSegmentsPerInterval;

  private TimeSegmentPruner _segmentPruner;
  private List<String> _segments;
  private Set<String> _onlineSegments;
  private BrokerRequest _brokerRequest;
  private int _numIntervals;
  private int _cursor;

  @Setup(Level.Iteration)
  public void setUp() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN).build();
    DateTimeFieldSpec timeFieldSpec =
        new DateTimeFieldSpec(TIME_COLUMN, DataType.LONG, "EPOCH|MILLISECONDS", "1:MILLISECONDS");
    _segmentPruner = new TimeSegmentPruner(tableConfig, timeFieldSpec);

    _segments = new ArrayList<>(_numSegments);
    List<ZNRecord> znRecords = new ArrayList<>(_numSegments);
    for (int i = 0; i < _numSegments; i++) {
      String segment = RAW_TABLE_NAME + "__" + (i % 64) + "__" + (i / 64) + "__20240101T0000Z";
      long startTimeMs = BASE_TIME_MS + (long) (i / _numSegmentsPerInterval) * SEGMENT_SPAN_MS;
      _segments.add(segment);
      znRecords.add(createZNRecord(segment, startTimeMs, startTimeMs + SEGMENT_SPAN_MS - 1));
    }
    // NOTE: Ideal state and external view are not used by TimeSegmentPruner
    _segmentPruner.init(null, null, _segments, znRecords);
    _onlineSegments = new HashSet<>(_segments);
    _numIntervals = (_numSegments + _numSegmentsPerInterval - 1) / _numSegmentsPerInterval;

    long queryStartMs = BASE_TIME_MS + (_numIntervals / 4L) * SEGMENT_SPAN_MS;
    _brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(
        "SELECT * FROM " + RAW_TABLE_NAME + " WHERE " + TIME_COLUMN + " BETWEEN " + queryStartMs + " AND " + (
            queryStartMs + 24 * SEGMENT_SPAN_MS));
    _cursor = 0;
  }

  private static ZNRecord createZNRecord(String segment, long startTimeMs, long endTimeMs) {
    ZNRecord znRecord = new ZNRecord(segment);
    znRecord.setLongField(CommonConstants.Segment.START_TIME, startTimeMs);
    znRecord.setLongField(CommonConstants.Segment.END_TIME, endTimeMs);
    znRecord.setEnumField(CommonConstants.Segment.TIME_UNIT, TimeUnit.MILLISECONDS);
    return znRecord;
  }

  /// Refreshes a segment onto a time range it did not have before - the REALTIME commit shape.
  ///
  /// The segment is moved one time bucket forward rather than past the end of the table, so that the number of
  /// distinct intervals and the selectivity of the benchmark query both stay at the values `_numSegments` and
  /// `_numSegmentsPerInterval` model, however long the benchmark runs.
  private void refreshChanged() {
    int index = _cursor++;
    int segmentIndex = index % _numSegments;
    String segment = _segments.get(segmentIndex);
    int intervalIndex = (segmentIndex / _numSegmentsPerInterval + 1 + index / _numSegments) % _numIntervals;
    long startTimeMs = BASE_TIME_MS + (long) intervalIndex * SEGMENT_SPAN_MS;
    _segmentPruner.refreshSegment(segment, createZNRecord(segment, startTimeMs, startTimeMs + SEGMENT_SPAN_MS - 1));
  }

  @Benchmark
  public void refreshChangedInterval() {
    refreshChanged();
  }

  /// Refreshes a segment onto the time range it already has - the OFFLINE re-push shape.
  @Benchmark
  public void refreshUnchangedInterval() {
    int index = _cursor++ % _numSegments;
    String segment = _segments.get(index);
    long startTimeMs = BASE_TIME_MS + (long) (index / _numSegmentsPerInterval) * SEGMENT_SPAN_MS;
    _segmentPruner.refreshSegment(segment, createZNRecord(segment, startTimeMs, startTimeMs + SEGMENT_SPAN_MS - 1));
  }

  /// A full production cycle: the refreshes one broker absorbs between two queries it serves, then the query.
  @Benchmark
  public void refreshBurstThenPrune(Blackhole blackhole) {
    for (int i = 0; i < REFRESHES_PER_QUERY; i++) {
      refreshChanged();
    }
    blackhole.consume(_segmentPruner.prune(_brokerRequest, _onlineSegments));
  }

  /// Query path only, to confirm pruning itself does not regress.
  @Benchmark
  public void prune(Blackhole blackhole) {
    blackhole.consume(_segmentPruner.prune(_brokerRequest, _onlineSegments));
  }

  /// The latency one query pays when it is the first to read the tree after a segment change. This is the cost the
  /// lazy rebuild moves onto the query path, so measure it on its own rather than blended into a throughput number.
  /// The refresh itself is a fraction of a microsecond and does not move the result.
  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void pruneAfterSegmentChange(Blackhole blackhole) {
    refreshChanged();
    blackhole.consume(_segmentPruner.prune(_brokerRequest, _onlineSegments));
  }

  public static void main(String[] args)
      throws RunnerException {
    new Runner(new OptionsBuilder().include(BenchmarkTimeSegmentPruner.class.getSimpleName()).addProfiler("gc").build())
        .run();
  }
}
