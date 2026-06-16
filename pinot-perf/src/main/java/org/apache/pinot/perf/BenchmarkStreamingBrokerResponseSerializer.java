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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.broker.api.resources.StreamingBrokerResponseJacksonSerializer;
import org.apache.pinot.common.response.EagerToLazyBrokerResponseAdaptor;
import org.apache.pinot.common.response.StreamingBrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.JsonUtils;
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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Pure in-memory JMH benchmark comparing three broker-response serialization paths.
 *
 * <p>Three modes are exercised:
 * <ol>
 *   <li><b>eagerDirect</b> — "master-style" path: {@code mapper.writeValue(eagerResponse)} using a plain
 *       {@link ObjectMapper} with no streaming module registered, writing to a null sink. This is the
 *       baseline that existed before the streaming path was introduced.</li>
 *   <li><b>eagerViaStreaming</b> — eager response wrapped in {@link EagerToLazyBrokerResponseAdaptor} and
 *       serialized through the {@link StreamingBrokerResponseJacksonSerializer}. This is the path taken when
 *       streaming is enabled but the broker produced a fully-materialized eager response (note: streaming
 *       defaults to {@code false} in production).</li>
 *   <li><b>listStreaming</b> — {@link StreamingBrokerResponse.ListStreamingBrokerResponse} fed directly to
 *       the streaming serializer (the true streaming path used by MSE).</li>
 * </ol>
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code rows} — 1 000 or 100 000 rows per response.</li>
 *   <li>{@code nullFraction} — 0.0 (no nulls) or 0.2 (20 % nulls per column).</li>
 * </ul>
 *
 * <p>Columns are a fixed mix covering each interesting serialization case:
 * INT, LONG, DOUBLE, STRING, TIMESTAMP, BYTES (pre-formatted hex), and BIG_DECIMAL.
 *
 * <p>To run with GC profiling:
 * <pre>
 *   java -jar pinot-perf/target/pinot-perf-*-shaded.jar BenchmarkStreamingBrokerResponseSerializer -prof gc
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 3)
@State(Scope.Benchmark)
public class BenchmarkStreamingBrokerResponseSerializer {

  // ---- column layout -------------------------------------------------------

  private static final String[] COLUMN_NAMES = {
      "intCol", "longCol", "doubleCol", "stringCol", "tsCol", "bytesCol", "bigDecimalCol"
  };

  private static final DataSchema.ColumnDataType[] COLUMN_TYPES = {
      DataSchema.ColumnDataType.INT,
      DataSchema.ColumnDataType.LONG,
      DataSchema.ColumnDataType.DOUBLE,
      DataSchema.ColumnDataType.STRING,
      // TIMESTAMP external type is String (formatted). The serializer formats Timestamp objects; we supply
      // pre-formatted strings (as produced by the MSE result-set path) to avoid measuring formatting overhead.
      DataSchema.ColumnDataType.TIMESTAMP,
      // BYTES external type is String (hex). We supply pre-formatted hex strings.
      DataSchema.ColumnDataType.BYTES,
      DataSchema.ColumnDataType.BIG_DECIMAL
  };

  private static final DataSchema DATA_SCHEMA = new DataSchema(COLUMN_NAMES, COLUMN_TYPES);

  // ---- benchmark parameters ------------------------------------------------

  @Param({"1000", "100000"})
  int _rows;

  @Param({"0.0", "0.2"})
  double _nullFraction;

  // ---- shared state built at setup -----------------------------------------

  /** Rows shared by all three benchmark methods. */
  private List<Object[]> _rowList;

  /** Eager response used by modes (a) and (b). */
  private BrokerResponseNativeV2 _eagerResponse;

  /** ObjectMapper for mode (a): no streaming module — plain Jackson serialization. */
  private ObjectMapper _eagerMapper;

  /** ObjectMapper for modes (b) and (c): streaming module registered. */
  private ObjectMapper _streamingMapper;

  /** Minimal metainfo (empty exceptions) used by modes (b) and (c). */
  private StreamingBrokerResponse.Metainfo _metainfo;

  @Setup(Level.Trial)
  public void setUp() {
    Random rng = new Random(42);

    // --- build row data ---
    _rowList = new ArrayList<>(_rows);
    long baseTimestamp = 1_700_000_000_000L; // ~2023-11-14 as millis
    for (int i = 0; i < _rows; i++) {
      Object[] row = new Object[COLUMN_NAMES.length];
      // INT
      row[0] = maybeNull(rng, _nullFraction, rng.nextInt());
      // LONG
      row[1] = maybeNull(rng, _nullFraction, (long) i);
      // DOUBLE
      row[2] = maybeNull(rng, _nullFraction, rng.nextDouble() * 1_000_000);
      // STRING
      row[3] = maybeNull(rng, _nullFraction, "str_" + i);
      // TIMESTAMP — supply a pre-formatted String matching Timestamp.toString() output
      row[4] = maybeNull(rng, _nullFraction, new Timestamp(baseTimestamp + i).toString());
      // BYTES — supply a pre-formatted hex string (as returned by the external type path)
      row[5] = maybeNull(rng, _nullFraction, String.format("%08x", i));
      // BIG_DECIMAL — use a variety of scales
      row[6] = maybeNull(rng, _nullFraction, new BigDecimal(i).setScale(rng.nextInt(5)));
      _rowList.add(row);
    }

    // --- eager response ---
    ResultTable resultTable = new ResultTable(DATA_SCHEMA, _rowList);
    _eagerResponse = new BrokerResponseNativeV2();
    _eagerResponse.setResultTable(resultTable);

    // --- mappers ---
    // Mode (a): plain ObjectMapper; no streaming module.
    _eagerMapper = JsonUtils.createMapper();

    // Modes (b) and (c): streaming module registered, matching production setup in PinotClientRequest.
    _streamingMapper = JsonUtils.createMapper();
    StreamingBrokerResponseJacksonSerializer.registerModule(_streamingMapper, Comparator.naturalOrder());

    // --- simple metainfo returning empty exceptions ---
    _metainfo = buildSimpleMetainfo();
  }

  // ---- benchmarks ----------------------------------------------------------

  /**
   * Mode (a) — master-style: serialize the eager {@link BrokerResponseNativeV2} directly via a plain
   * {@link ObjectMapper} to a null sink. This is the baseline that predates the streaming path.
   */
  @Benchmark
  public void eagerDirect()
      throws IOException {
    _eagerMapper.writeValue(OutputStream.nullOutputStream(), _eagerResponse);
  }

  /**
   * Mode (b) — eager-via-streaming: wrap the eager response in {@link EagerToLazyBrokerResponseAdaptor}
   * and serialize it through {@link StreamingBrokerResponseJacksonSerializer}. This is the path taken when
   * streaming is enabled but the broker produced a fully-materialized eager response (streaming defaults to
   * {@code false} in production).
   */
  @Benchmark
  public void eagerViaStreaming()
      throws IOException {
    EagerToLazyBrokerResponseAdaptor adaptor = new EagerToLazyBrokerResponseAdaptor(_eagerResponse);
    _streamingMapper.writeValue(OutputStream.nullOutputStream(), adaptor);
  }

  /**
   * Mode (c) — list-streaming: serialize a {@link StreamingBrokerResponse.ListStreamingBrokerResponse}
   * through {@link StreamingBrokerResponseJacksonSerializer}. This is the true streaming path used by
   * MSE queries.
   */
  @Benchmark
  public void listStreaming()
      throws IOException {
    StreamingBrokerResponse response =
        new StreamingBrokerResponse.ListStreamingBrokerResponse(DATA_SCHEMA, _metainfo, _rowList);
    _streamingMapper.writeValue(OutputStream.nullOutputStream(), response);
  }

  // ---- helpers -------------------------------------------------------------

  /**
   * Returns {@code value} or {@code null} with probability {@code nullFraction}.
   */
  private static Object maybeNull(Random rng, double nullFraction, Object value) {
    return (nullFraction > 0 && rng.nextDouble() < nullFraction) ? null : value;
  }

  /**
   * Builds a {@link StreamingBrokerResponse.Metainfo} with empty exceptions, suitable for benchmark use.
   */
  private static StreamingBrokerResponse.Metainfo buildSimpleMetainfo() {
    List<QueryProcessingException> emptyExceptions = Collections.emptyList();
    return new StreamingBrokerResponse.Metainfo() {
      @Override
      public List<QueryProcessingException> getExceptions() {
        return emptyExceptions;
      }

      @Override
      public ObjectNode asJson() {
        ObjectNode node = JsonUtils.newObjectNode();
        node.set("exceptions", JsonUtils.objectToJsonNode(emptyExceptions));
        return node;
      }
    };
  }

  // ---- entry point ---------------------------------------------------------

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(BenchmarkStreamingBrokerResponseSerializer.class.getSimpleName())
        .addProfiler(GCProfiler.class);
    new Runner(opt.build()).run();
  }
}
