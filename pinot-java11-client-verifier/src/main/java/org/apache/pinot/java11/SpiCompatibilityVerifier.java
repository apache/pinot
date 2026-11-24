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
package org.apache.pinot.java11;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.serde.TimeSeriesPlanSerde;


/// Exercises the standalone SPI artifacts and scans each artifact's runtime classpath on Java 11.
/// Each invocation runs on the calling thread and retains no shared state.
public final class SpiCompatibilityVerifier {
  private static final int JAVA_VERSION = 11;
  private static final String SAMPLE_TABLE_NAME = "baseballStats";

  private SpiCompatibilityVerifier() {
  }

  public static void main(String[] args)
      throws IOException {
    require(args.length == 1, "usage: SpiCompatibilityVerifier <artifactId>");
    require(Runtime.version().feature() == JAVA_VERSION, "expected Java %d, got %s", JAVA_VERSION, Runtime.version());

    String artifactId = args[0];
    ClasspathClosureScanner.Result result =
        ClasspathClosureScanner.scan(System.getProperty("java.class.path"), JAVA_VERSION);
    require(result.getArchivesScanned() > 1 && result.getClassFilesInArchives() > 0,
        "expected %s and its dependencies on the runtime classpath", artifactId);
    ClasspathClosureScanner.assertLoadable(result, JAVA_VERSION, artifactId);

    switch (artifactId) {
      case "pinot-spi":
        Core.checkSpiSchemaDeserialization();
        Core.checkSpiTableConfigDeserialization();
        break;
      case "pinot-segment-spi":
        Segment.checkSegmentSpiDataBuffer();
        break;
      case "pinot-timeseries-spi":
        TimeSeries.checkTimeSeriesSpiPlanSerde();
        break;
      default:
        throw new IllegalArgumentException("Unexpected SPI artifact: " + artifactId);
    }
    System.out.printf("%s passed runtime checks on Java %d (%d archives, %d class files scanned)%n", artifactId,
        JAVA_VERSION, result.getArchivesScanned(), result.getClassFilesInArchives());
  }

  // Keep each set of checks in its own class so verification can load it on the standalone artifact's classpath.
  static final class Core {
    private Core() {
    }

    /// pinot-spi schema deserialization, which is Jackson plus the SPI's own field spec model.
    static void checkSpiSchemaDeserialization()
        throws IOException {
      String schemaJson = "{"
          + "\"schemaName\":\"" + SAMPLE_TABLE_NAME + "\","
          + "\"dimensionFieldSpecs\":["
          + "{\"name\":\"playerName\",\"dataType\":\"STRING\"},"
          + "{\"name\":\"teams\",\"dataType\":\"STRING\",\"singleValueField\":false}],"
          + "\"metricFieldSpecs\":[{\"name\":\"numGames\",\"dataType\":\"LONG\"}],"
          + "\"dateTimeFieldSpecs\":[{\"name\":\"gameDate\",\"dataType\":\"LONG\","
          + "\"format\":\"1:MILLISECONDS:EPOCH\",\"granularity\":\"1:DAYS\"}]}";

      Schema schema = Schema.fromString(schemaJson);
      require(SAMPLE_TABLE_NAME.equals(schema.getSchemaName()), "unexpected schema name: %s", schema.getSchemaName());
      require(schema.getColumnNames().size() == 4, "expected 4 columns, got %s", schema.getColumnNames());
      require(schema.getFieldSpecFor("playerName").getDataType() == DataType.STRING, "playerName should be STRING");
      require(!schema.getFieldSpecFor("teams").isSingleValueField(), "teams should be multi-valued");
      require(schema.getDateTimeSpec("gameDate") != null, "gameDate date-time spec missing");

      Schema reparsed = Schema.fromString(schema.toSingleLineJsonString());
      require(schema.equals(reparsed), "schema did not survive a JSON round trip");
    }

    /// pinot-spi table config deserialization, the other half of what a third-party plugin reads.
    static void checkSpiTableConfigDeserialization()
        throws IOException {
      String tableConfigJson = "{"
          + "\"tableName\":\"" + SAMPLE_TABLE_NAME + "\","
          + "\"tableType\":\"OFFLINE\","
          + "\"segmentsConfig\":{\"replication\":\"2\",\"timeColumnName\":\"gameDate\"},"
          + "\"tenants\":{\"broker\":\"DefaultTenant\",\"server\":\"DefaultTenant\"},"
          + "\"tableIndexConfig\":{\"invertedIndexColumns\":[\"playerName\"],\"loadMode\":\"MMAP\"},"
          + "\"metadata\":{}}";

      TableConfig tableConfig = JsonUtils.stringToObject(tableConfigJson, TableConfig.class);
      require(tableConfig.getTableType() == TableType.OFFLINE, "unexpected table type: %s", tableConfig.getTableType());
      require((SAMPLE_TABLE_NAME + "_OFFLINE").equals(tableConfig.getTableName()), "unexpected table name: %s",
          tableConfig.getTableName());
      require("2".equals(tableConfig.getValidationConfig().getReplication()), "unexpected replication: %s",
          tableConfig.getValidationConfig().getReplication());
      require(tableConfig.getIndexingConfig().getInvertedIndexColumns().contains("playerName"),
          "inverted index column lost: %s", tableConfig.getIndexingConfig().getInvertedIndexColumns());

      TableConfig reserialized = JsonUtils.stringToObject(JsonUtils.objectToString(tableConfig), TableConfig.class);
      require(tableConfig.getTableName().equals(reserialized.getTableName()),
          "table config did not survive a JSON round trip");
    }
  }

  static final class Segment {
    private Segment() {
    }

    /// pinot-segment-spi's off-heap buffer, which is what a third-party index or reader plugin built against the SPI
    /// actually uses. Worth exercising rather than merely scanning: the allocation path goes through `Unsafe` and
    /// `sun.misc.Cleaner` reflection whose availability is exactly what shifts between JDK releases, and the
    /// class-file scan cannot see that.
    static void checkSegmentSpiDataBuffer()
        throws IOException {
      int size = 64;
      try (PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(size, ByteOrder.LITTLE_ENDIAN,
          "spi-compatibility-verifier")) {
        require(buffer.size() == size, "unexpected buffer size: %d", buffer.size());
        buffer.putInt(0, 0xCAFEBABE);
        buffer.putLong(8, 1750000000000L);
        buffer.putDouble(16, 0.305d);
        require(buffer.getInt(0) == 0xCAFEBABE, "int lost in the off-heap buffer: %d", buffer.getInt(0));
        require(buffer.getLong(8) == 1750000000000L, "long lost in the off-heap buffer: %d", buffer.getLong(8));
        require(buffer.getDouble(16) == 0.305d, "double lost in the off-heap buffer: %s", buffer.getDouble(16));

        byte[] bytes = "baseballStats".getBytes(StandardCharsets.UTF_8);
        buffer.readFrom(24, bytes, 0, bytes.length);
        byte[] readBack = new byte[bytes.length];
        buffer.copyTo(24, readBack, 0, readBack.length);
        require(Arrays.equals(bytes, readBack), "bytes lost in the off-heap buffer: %s",
            new String(readBack, StandardCharsets.UTF_8));
      }
    }
  }

  static final class TimeSeries {
    private TimeSeries() {
    }

    /// pinot-timeseries-spi's plan model, which a time series language plugin serializes and ships.
    static void checkTimeSeriesSpiPlanSerde() {
      LeafTimeSeriesPlanNode leaf = new LeafTimeSeriesPlanNode("sfp#0", List.of(), SAMPLE_TABLE_NAME, "gameDate",
          TimeUnit.MILLISECONDS, 0L, "battingAverage > 0.3", "numGames",
          new AggInfo("SUM", false, Map.of("window", "60")), List.of("playerName"), 100, Map.of("timeoutMs", "20000"));

      String serialized = TimeSeriesPlanSerde.serialize(leaf);
      require(serialized.contains(SAMPLE_TABLE_NAME), "table name lost during plan serialization: %s", serialized);

      BaseTimeSeriesPlanNode deserialized = TimeSeriesPlanSerde.deserialize(serialized);
      require(deserialized instanceof LeafTimeSeriesPlanNode, "unexpected plan node type: %s",
          deserialized.getClass().getName());
      LeafTimeSeriesPlanNode roundTripped = (LeafTimeSeriesPlanNode) deserialized;
      require(SAMPLE_TABLE_NAME.equals(roundTripped.getTableName()), "unexpected table name: %s",
          roundTripped.getTableName());
      require("numGames".equals(roundTripped.getValueExpression()), "unexpected value expression: %s",
          roundTripped.getValueExpression());
      require(roundTripped.getAggInfo() != null && "SUM".equals(roundTripped.getAggInfo().getAggFunction()),
          "aggregation lost during the plan round trip: %s", roundTripped.getAggInfo());
      require(List.of("playerName").equals(roundTripped.getGroupByExpressions()), "group by lost: %s",
          roundTripped.getGroupByExpressions());
      require(roundTripped.getLimit() == 100, "unexpected limit: %d", roundTripped.getLimit());

      TimeBuckets timeBuckets = TimeBuckets.ofSeconds(1750000000L, Duration.ofSeconds(60), 10);
      require(timeBuckets.getNumBuckets() == 10, "unexpected bucket count: %d", timeBuckets.getNumBuckets());
      require(timeBuckets.getTimeBuckets().length == 10, "unexpected bucket array length: %d",
          timeBuckets.getTimeBuckets().length);
    }
  }

  private static void require(boolean condition, String message, Object... args) {
    if (!condition) {
      throw new AssertionError(args.length == 0 ? message : String.format(message, args));
    }
  }
}
