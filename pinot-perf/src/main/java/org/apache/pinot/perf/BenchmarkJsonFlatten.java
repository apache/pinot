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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.MapUtils;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Compares the cost of building the JSON index input for a realtime row from a re-serialized JSON <b>string</b>
 * (the current path: {@code DataTypeTransformer} serializes the Map, then the JSON index re-parses it) versus from the
 * already-parsed <b>Map</b> (the parse-once cache: the index flattens the Map directly via {@code valueToTree}).
 *
 * <ul>
 *   <li>{@code flattenFromString} - current per-row index cost ({@code stringToJsonNode} + flatten).</li>
 *   <li>{@code flattenFromMap}    - parse-once cost ({@code valueToTree} + flatten), the re-parse avoided.</li>
 *   <li>{@code serializeMap}      - the Map to String serialization that stays (the forward index stores it).</li>
 * </ul>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
public class BenchmarkJsonFlatten {

  // A representative structured log line (nested "log" object, ids, an array) similar to the zomato `message` field.
  private static final String MESSAGE_JSON =
      "{\"x_gr_trace_id\":\"a1b2c3d4-e5f6-7890-abcd-ef1234567890\","
          + "\"user_id\":\"user-99174432\","
          + "\"path\":\"/retail_purchase_order/modules/serialized_inventory.py\","
          + "\"status\":\"500\",\"event\":\"request.error\",\"level\":\"ERROR\","
          + "\"stream\":\"stderr\",\"time\":\"2026-06-12T10:31:13.123456789Z\","
          + "\"msg\":\"File \\\"/retail_purchase_order/modules/serialized_inventory.py\\\", line 222, in "
          + "create_integration_partner_entries\","
          + "\"log\":{\"msg\":\"create_integration_partner_entries failed\",\"message\":\"partner missing\","
          + "\"level\":\"error\",\"logger\":\"retail.purchase_order.serialized_inventory\",\"line\":222},"
          + "\"http\":{\"method\":\"POST\",\"status_code\":500,\"duration_ms\":1843,\"host\":\"api-internal\"},"
          + "\"tags\":[\"retail\",\"purchase_order\",\"serialized_inventory\",\"prod\"]}";

  private Map<String, Object> _messageMap;
  private JsonIndexConfig _config;

  @Setup
  public void setup()
      throws Exception {
    _messageMap = JsonUtils.stringToObject(MESSAGE_JSON, Map.class);
    _config = new JsonIndexConfig();
    _config.setMaxLevels(2);
  }

  @Benchmark
  public Object flattenFromString()
      throws Exception {
    return JsonUtils.flatten(MESSAGE_JSON, _config);
  }

  @Benchmark
  public Object flattenFromMap()
      throws Exception {
    return JsonUtils.flattenParsed(_messageMap, _config);
  }

  @Benchmark
  public Object serializeMap() {
    return MapUtils.toString(_messageMap);
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkJsonFlatten.class.getSimpleName()).build()).run();
  }
}
