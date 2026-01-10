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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Benchmark to compare JSON parsing performance:
 * - Old approach: bytesToJsonNode() + jsonNodeToMap() (two-step parsing)
 * - New approach: bytesToMap() (single-step parsing)
 *
 * This benchmark simulates the JSON decoding path in RealtimeSegmentDataManager.consumeLoop()
 * where JSONMessageDecoder.decode() is called for each message from the stream.
 *
 * Run with: mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath org.apache.pinot.perf
 * .BenchmarkJsonParsing"
 * Or compile and run: java -jar pinot-perf/target/pinot-perf-*-shaded.jar BenchmarkJsonParsing
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@State(Scope.Benchmark)
public class BenchmarkJsonParsing {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(BenchmarkJsonParsing.class.getSimpleName())
        .shouldDoGC(true);
    new Runner(opt.build()).run();
  }

  // Simulates different JSON payload sizes
  @Param({"small", "medium", "large", "nested"})
  private String _payloadType;

  // Number of JSON messages to pre-generate for the benchmark
  private static final int NUM_MESSAGES = 1000;

  private List<byte[]> _jsonPayloads;
  private int _currentIndex = 0;

  @Setup(Level.Trial)
  public void setUp() {
    _jsonPayloads = new ArrayList<>(NUM_MESSAGES);
    Random random = new Random(42);

    for (int i = 0; i < NUM_MESSAGES; i++) {
      String json = generateJsonPayload(_payloadType, random, i);
      _jsonPayloads.add(json.getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * Generates different types of JSON payloads to simulate real-world streaming data.
   */
  private String generateJsonPayload(String type, Random random, int index) {
    switch (type) {
      case "small":
        // Small payload: ~100 bytes, typical for simple events
        return String.format(
            "{\"id\":%d,\"timestamp\":%d,\"value\":%.2f,\"status\":\"%s\"}",
            index,
            System.currentTimeMillis() + random.nextInt(10000),
            random.nextDouble() * 1000,
            random.nextBoolean() ? "active" : "inactive"
        );

      case "medium":
        // Medium payload: ~500 bytes, typical for user events
        return String.format(
            "{\"eventId\":\"%s-%d\",\"userId\":\"%s\",\"sessionId\":\"%s\","
                + "\"timestamp\":%d,\"eventType\":\"%s\",\"properties\":{"
                + "\"page\":\"/products/%d\",\"referrer\":\"https://example.com/search?q=%s\","
                + "\"duration\":%d,\"scrollDepth\":%.2f,\"clicks\":%d},"
                + "\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36\","
                + "\"ipAddress\":\"192.168.%d.%d\",\"country\":\"US\",\"city\":\"San Francisco\"}",
            "evt", index,
            "user-" + random.nextInt(100000),
            "sess-" + random.nextInt(1000000),
            System.currentTimeMillis(),
            randomEventType(random),
            random.nextInt(10000),
            randomSearchQuery(random),
            random.nextInt(300),
            random.nextDouble() * 100,
            random.nextInt(20),
            random.nextInt(256),
            random.nextInt(256)
        );

      case "large":
        // Large payload: ~2KB, typical for detailed analytics events
        StringBuilder sb = new StringBuilder();
        sb.append("{\"eventId\":\"").append("evt-").append(index).append("\",");
        sb.append("\"timestamp\":").append(System.currentTimeMillis()).append(",");
        sb.append("\"data\":{");

        // Add 20 fields to make it large
        for (int j = 0; j < 20; j++) {
          if (j > 0) {
            sb.append(",");
          }
          sb.append("\"field").append(j).append("\":\"")
              .append(randomString(random, 50 + random.nextInt(50))).append("\"");
        }
        sb.append("},\"metrics\":{");

        // Add 10 numeric metrics
        for (int j = 0; j < 10; j++) {
          if (j > 0) {
            sb.append(",");
          }
          sb.append("\"metric").append(j).append("\":").append(random.nextDouble() * 1000);
        }
        sb.append("},\"tags\":[");

        // Add 5 tags
        for (int j = 0; j < 5; j++) {
          if (j > 0) {
            sb.append(",");
          }
          sb.append("\"tag-").append(random.nextInt(100)).append("\"");
        }
        sb.append("]}");
        return sb.toString();

      case "nested":
        // Nested payload: ~1KB with nested objects and arrays
        return String.format(
            "{\"order\":{\"id\":%d,\"customer\":{\"id\":\"%s\",\"name\":\"%s\",\"email\":\"%s@example.com\"},"
                + "\"items\":[{\"sku\":\"SKU-%d\",\"name\":\"%s\",\"price\":%.2f,\"qty\":%d},"
                + "{\"sku\":\"SKU-%d\",\"name\":\"%s\",\"price\":%.2f,\"qty\":%d},"
                + "{\"sku\":\"SKU-%d\",\"name\":\"%s\",\"price\":%.2f,\"qty\":%d}],"
                + "\"shipping\":{\"method\":\"%s\",\"address\":{\"street\":\"%s\",\"city\":\"%s\","
                + "\"state\":\"%s\",\"zip\":\"%05d\"}},\"total\":%.2f,\"currency\":\"USD\"}}",
            index,
            "cust-" + random.nextInt(10000),
            randomName(random),
            "user" + random.nextInt(10000),
            random.nextInt(10000), randomProduct(random), random.nextDouble() * 100, random.nextInt(5) + 1,
            random.nextInt(10000), randomProduct(random), random.nextDouble() * 100, random.nextInt(5) + 1,
            random.nextInt(10000), randomProduct(random), random.nextDouble() * 100, random.nextInt(5) + 1,
            random.nextBoolean() ? "express" : "standard",
            random.nextInt(9999) + " Main St",
            randomCity(random),
            randomState(random),
            random.nextInt(99999),
            random.nextDouble() * 500 + 50
        );

      default:
        return "{\"id\":" + index + "}";
    }
  }

  private byte[] getNextPayload() {
    byte[] payload = _jsonPayloads.get(_currentIndex);
    _currentIndex = (_currentIndex + 1) % NUM_MESSAGES;
    return payload;
  }

  /**
   * OLD APPROACH: Two-step parsing (bytesToJsonNode + jsonNodeToMap)
   * This was the original implementation that caused high CPU usage.
   */
  @Benchmark
  public Map<String, Object> oldApproachTwoStepParsing(Blackhole blackhole)
      throws IOException {
    byte[] payload = getNextPayload();

    // Step 1: Parse bytes to JsonNode (intermediate tree representation)
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(payload, 0, payload.length);

    // Step 2: Convert JsonNode to Map
    Map<String, Object> result = JsonUtils.jsonNodeToMap(jsonNode);

    blackhole.consume(jsonNode);
    return result;
  }

  /**
   * NEW APPROACH: Single-step parsing (bytesToMap)
   * This is the optimized implementation that parses directly to Map.
   */
  @Benchmark
  public Map<String, Object> newApproachDirectParsing(Blackhole blackhole)
      throws IOException {
    byte[] payload = getNextPayload();

    // Direct parsing to Map, skipping the intermediate JsonNode
    Map<String, Object> result = JsonUtils.bytesToMap(payload, 0, payload.length);

    return result;
  }

  // Helper methods for generating realistic test data

  private static String randomEventType(Random random) {
    String[] types = {"pageview", "click", "scroll", "purchase", "signup", "login", "search"};
    return types[random.nextInt(types.length)];
  }

  private static String randomSearchQuery(Random random) {
    String[] queries = {"laptop", "phone", "headphones", "camera", "tablet", "watch", "speaker"};
    return queries[random.nextInt(queries.length)];
  }

  private static String randomString(Random random, int length) {
    StringBuilder sb = new StringBuilder(length);
    String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (int i = 0; i < length; i++) {
      sb.append(chars.charAt(random.nextInt(chars.length())));
    }
    return sb.toString();
  }

  private static String randomName(Random random) {
    String[] firstNames = {"John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank"};
    String[] lastNames = {"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"};
    return firstNames[random.nextInt(firstNames.length)] + " " + lastNames[random.nextInt(lastNames.length)];
  }

  private static String randomProduct(Random random) {
    String[] products = {"Widget", "Gadget", "Device", "Tool", "Component", "Module", "Unit"};
    String[] adjectives = {"Premium", "Standard", "Basic", "Pro", "Elite", "Ultra"};
    return adjectives[random.nextInt(adjectives.length)] + " " + products[random.nextInt(products.length)];
  }

  private static String randomCity(Random random) {
    String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "San Francisco", "Seattle"};
    return cities[random.nextInt(cities.length)];
  }

  private static String randomState(Random random) {
    String[] states = {"NY", "CA", "IL", "TX", "AZ", "WA", "OR", "NV", "CO", "FL"};
    return states[random.nextInt(states.length)];
  }
}
