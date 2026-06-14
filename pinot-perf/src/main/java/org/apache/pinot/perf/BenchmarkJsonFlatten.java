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

import java.nio.charset.StandardCharsets;
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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Compares the cost of building the JSON index input for a realtime row from a re-serialized JSON `String`
/// (the current path: `DataTypeTransformer` serializes the `Map`, then the JSON index re-parses it) versus from the
/// already-parsed `Map` (the parse-once cache: the index flattens the `Map` directly via `valueToTree`).
///
/// Measured operations:
/// - `flattenFromString` - current per-row index cost (`stringToJsonNode` + flatten).
/// - `flattenFromMap` - parse-once cost (`valueToTree` + flatten), the re-parse avoided.
/// - `serializeMap` - the `Map` to `String` serialization that stays (the forward index stores it).
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
public class BenchmarkJsonFlatten {
  private static final String PAYLOAD_PREFIX =
      "{\"x_gr_trace_id\":\"a1b2c3d4-e5f6-7890-abcd-ef1234567890\","
          + "\"user_id\":\"user-99174432\","
          + "\"path\":\"/retail_purchase_order/modules/serialized_inventory.py\","
          + "\"status\":\"500\","
          + "\"event\":\"request.error\","
          + "\"level\":\"ERROR\","
          + "\"log\":{\"logger\":\"retail.purchase_order.serialized_inventory\",\"line\":222},"
          + "\"tags\":[\"retail\",\"prod\"],"
          + "\"stacktrace\":\"";
  private static final String PAYLOAD_SUFFIX = "\"}";
  private static final String STACK_FRAME =
      " at retail.purchase_order.serialized_inventory.create_integration_partner_entries"
          + "(serialized_inventory.py:222)\\n";

  @Param({"330", "2900", "8000"})
  private int _payloadBytes;

  private String _messageJson;
  private Map<String, Object> _messageMap;
  private JsonIndexConfig _config;

  @Setup
  public void setup()
      throws Exception {
    _messageJson = buildMessageJson(_payloadBytes);
    _messageMap = JsonUtils.stringToObject(_messageJson, Map.class);
    _config = new JsonIndexConfig();
    _config.setMaxLevels(2);
  }

  @Benchmark
  public Object flattenFromString()
      throws Exception {
    return JsonUtils.flatten(_messageJson, _config);
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

  private static String buildMessageJson(int targetBytes) {
    int fixedBytes = utf8Length(PAYLOAD_PREFIX) + utf8Length(PAYLOAD_SUFFIX);
    if (targetBytes < fixedBytes) {
      throw new IllegalArgumentException("Target payload size " + targetBytes + " is smaller than minimum "
          + fixedBytes);
    }
    int stacktraceBytes = targetBytes - fixedBytes;
    StringBuilder stacktrace = new StringBuilder(stacktraceBytes);
    while (stacktrace.length() + STACK_FRAME.length() <= stacktraceBytes) {
      stacktrace.append(STACK_FRAME);
    }
    while (stacktrace.length() < stacktraceBytes) {
      stacktrace.append('x');
    }
    String json = PAYLOAD_PREFIX + stacktrace + PAYLOAD_SUFFIX;
    if (utf8Length(json) != targetBytes) {
      throw new IllegalStateException("Expected payload size " + targetBytes + " but built " + utf8Length(json));
    }
    return json;
  }

  private static int utf8Length(String value) {
    return value.getBytes(StandardCharsets.UTF_8).length;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkJsonFlatten.class.getSimpleName()).build()).run();
  }
}
