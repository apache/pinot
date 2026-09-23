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
package org.apache.pinot.query.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.proto.Worker;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Measures the two wire encodings of a leaf-stage worker's segment list: the legacy JSON custom property and the
/// native protobuf fields, on the broker (encode) and on the server (decode).
///
/// The measured unit is one leaf-stage worker, which is what the broker encodes once per worker per query in
/// [QueryPlanSerDeUtils#toProtoWorkerMetadataList] and a server decodes once per worker in
/// `fromProtoWorkerMetadata`. Encode includes `toByteArray`, because protobuf defers the UTF-8 encoding of its
/// strings to serialization while the JSON path materializes the whole string up front; leaving it out would flatter
/// the proto path. Decode starts from the bytes, as the server does, so it includes the protobuf parse.
///
/// Lives in the `org.apache.pinot.query.routing` package so that it can call the package-private decode entry point
/// the server uses, rather than widening it for a benchmark.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class BenchmarkSegmentListEncoding {
  /// Segment counts of one leaf-stage worker, from a small table to one worker of a very large table.
  @Param({"1000", "20000", "60000"})
  private int _numSegments;

  private List<WorkerMetadata> _workerMetadataList;
  private byte[] _legacyBytes;
  private byte[] _protoBytes;

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    List<String> segments = new ArrayList<>(_numSegments);
    for (int i = 0; i < _numSegments; i++) {
      // A realistic 60-character offline segment name.
      segments.add(String.format("airlineStats_OFFLINE_16071_16101_%027d", i));
    }
    WorkerMetadata workerMetadata = new WorkerMetadata(0,
        Map.of(1, new MailboxInfos(new MailboxInfo("localhost", 12345, List.of(0)))), new HashMap<>());
    workerMetadata.setTableSegmentsMap(Map.of("OFFLINE", segments));
    _workerMetadataList = List.of(workerMetadata);

    _legacyBytes = QueryPlanSerDeUtils.toProtoWorkerMetadataList(_workerMetadataList, false).get(0).toByteArray();
    _protoBytes = QueryPlanSerDeUtils.toProtoWorkerMetadataList(_workerMetadataList, true).get(0).toByteArray();
    System.out.printf("%n[%d segments] wire size: legacy %d bytes, proto %d bytes%n", _numSegments,
        _legacyBytes.length, _protoBytes.length);
  }

  /// Build plus `toByteArray`: the whole cost the broker pays per leaf-stage worker at dispatch.
  @Benchmark
  public int encodeLegacy() {
    return QueryPlanSerDeUtils.toProtoWorkerMetadataList(_workerMetadataList, false).get(0).toByteArray().length;
  }

  @Benchmark
  public int encodeProto() {
    return QueryPlanSerDeUtils.toProtoWorkerMetadataList(_workerMetadataList, true).get(0).toByteArray().length;
  }

  /// Build only, without `toByteArray`: the part the broker used to do on the query-compile executor at plan time.
  @Benchmark
  public Worker.WorkerMetadata buildLegacy() {
    return QueryPlanSerDeUtils.toProtoWorkerMetadataList(_workerMetadataList, false).get(0);
  }

  @Benchmark
  public Worker.WorkerMetadata buildProto() {
    return QueryPlanSerDeUtils.toProtoWorkerMetadataList(_workerMetadataList, true).get(0);
  }

  @Benchmark
  public WorkerMetadata decodeLegacy()
      throws Exception {
    return QueryPlanSerDeUtils.fromProtoWorkerMetadata(Worker.WorkerMetadata.parseFrom(_legacyBytes));
  }

  @Benchmark
  public WorkerMetadata decodeProto()
      throws Exception {
    return QueryPlanSerDeUtils.fromProtoWorkerMetadata(Worker.WorkerMetadata.parseFrom(_protoBytes));
  }

  public static void main(String[] args)
      throws RunnerException {
    new Runner(new OptionsBuilder().include(BenchmarkSegmentListEncoding.class.getSimpleName()).build()).run();
  }
}
