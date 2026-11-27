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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.ExternalView;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.ExecutionStats;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.PinotClientTransport;
import org.apache.pinot.client.PinotResultSet;
import org.apache.pinot.client.PreparedStatement;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.client.grpc.GrpcUtils;
import org.apache.pinot.common.compression.CompressionFactory;
import org.apache.pinot.common.compression.Compressor;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.encoder.ResponseEncoder;
import org.apache.pinot.common.response.encoder.ResponseEncoderFactory;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.grpc.BrokerGrpcQueryClient;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;


/// Exercises the Java and JDBC clients on the minimum supported JVM and scans the runtime classpath
/// for dependency bytecode that the JVM cannot load. The build itself requires JDK 25.
///
/// Usage: `java -cp <runtime closure> org.apache.pinot.java11.ClientCompatibilityVerifier [targetJavaFeatureVersion]`.
/// The target defaults to 17. Running on a different JVM fails, and the checks require no test framework on the
/// runtime classpath. The checks run sequentially on the calling thread.
public final class ClientCompatibilityVerifier {
  private static final int DEFAULT_TARGET_JAVA_FEATURE_VERSION = 17;
  private static final String SAMPLE_BROKER_RESPONSE_RESOURCE = "sample-broker-response.json";
  private static final String JDBC_SERVICE_DESCRIPTOR = "META-INF/services/java.sql.Driver";
  private static final String SAMPLE_TABLE_NAME = "baseballStats";
  private static final String JVM_VERSION_CHECK = "jvm-is-at-target-feature-version";

  /// The client runtimes must include the client libraries, common code and the standalone SPI artifacts.
  private static final List<String> REQUIRED_MODULES = List.of(
      "pinot-spi", "pinot-segment-spi", "pinot-timeseries-spi", "pinot-common", "pinot-java-client",
      "pinot-jdbc-client");

  /// Encoders and compression codecs that must stay covered. Iterating whatever the factories happen to return would
  /// let a refactor drop ARROW or ZSTD -- and with them Arrow memory and the JNI codecs, the most likely sources
  /// of a Java version floor bump -- while the job stayed green.
  private static final List<String> REQUIRED_RESPONSE_ENCODERS = List.of("JSON", "ARROW");
  private static final List<String> REQUIRED_COMPRESSION_CODECS = List.of("ZSTD", "SNAPPY", "LZ4", "GZIP", "DEFLATE");

  /// Floors for the closure scan's vacuity guards, set well below the real numbers (~220 jars, ~69k class files at the
  /// time of writing) so ordinary dependency churn does not trip them, but far enough above zero to catch a closure
  /// that failed to resolve.
  private static final int MIN_EXPECTED_ARCHIVES = 100;
  private static final int MIN_EXPECTED_CLASS_FILES_IN_ARCHIVES = 20_000;

  private final int _targetJavaFeatureVersion;
  private final String _sampleBrokerResponseJson;
  private final JsonNode _sampleBrokerResponse;

  private ClientCompatibilityVerifier(int targetJavaFeatureVersion)
      throws IOException {
    _targetJavaFeatureVersion = targetJavaFeatureVersion;
    _sampleBrokerResponseJson = readResource(SAMPLE_BROKER_RESPONSE_RESOURCE);
    _sampleBrokerResponse = JsonUtils.stringToJsonNode(_sampleBrokerResponseJson);
  }

  /// A single named verification. Throwing anything -- including [AssertionError] -- fails it.
  @FunctionalInterface
  private interface Check {
    void run()
        throws Exception;
  }

  public static void main(String[] args)
      throws IOException {
    int targetJavaFeatureVersion = DEFAULT_TARGET_JAVA_FEATURE_VERSION;
    if (args.length > 1) {
      throw new IllegalArgumentException(
          "Usage: ClientCompatibilityVerifier [targetJavaFeatureVersion], got: " + Arrays.toString(args));
    }
    if (args.length == 1) {
      targetJavaFeatureVersion = Integer.parseInt(args[0].trim());
    }
    System.exit(new ClientCompatibilityVerifier(targetJavaFeatureVersion).run());
  }

  private int run() {
    Map<String, Check> checks = new LinkedHashMap<>();
    // Guards against the whole job passing vacuously on the build JDK. Must stay first; failing it
    // aborts the run.
    checks.put(JVM_VERSION_CHECK, this::checkJvmIsAtTargetFeatureVersion);
    checks.put("classpath-closure-is-loadable", this::checkClasspathClosureIsLoadable);
    checks.put("spi-schema-deserialization", SpiCompatibilityVerifier.Core::checkSpiSchemaDeserialization);
    checks.put("spi-table-config-deserialization", SpiCompatibilityVerifier.Core::checkSpiTableConfigDeserialization);
    checks.put("segment-spi-data-buffer", SpiCompatibilityVerifier.Segment::checkSegmentSpiDataBuffer);
    checks.put("timeseries-spi-plan-serde", SpiCompatibilityVerifier.TimeSeries::checkTimeSeriesSpiPlanSerde);
    checks.put("common-data-schema-round-trip", this::checkCommonDataSchemaRoundTrip);
    checks.put("common-broker-response-deserialization", this::checkCommonBrokerResponseDeserialization);
    checks.put("common-response-encoders", this::checkCommonResponseEncoders);
    checks.put("common-calcite-sql-parsing", this::checkCommonCalciteSqlParsing);
    checks.put("common-helix-segment-metadata", this::checkCommonHelixSegmentMetadata);
    checks.put("common-grpc-response-decoding", this::checkCommonGrpcResponseDecoding);
    checks.put("common-grpc-channel-construction", this::checkCommonGrpcChannelConstruction);
    checks.put("java-client-http-transport", this::checkJavaClientHttpTransport);
    checks.put("java-client-query-execution", this::checkJavaClientQueryExecution);
    checks.put("java-client-prepared-statement", this::checkJavaClientPreparedStatement);
    checks.put("jdbc-driver-registration", this::checkJdbcDriverRegistration);
    checks.put("jdbc-result-set", this::checkJdbcResultSet);

    System.out.printf("Verifying Pinot clients on %s %s (%s), targeting Java %d%n", System.getProperty("java.vm.name"),
        System.getProperty("java.version"), System.getProperty("java.vendor"), _targetJavaFeatureVersion);
    System.out.println();

    List<String> failed = new ArrayList<>();
    for (Map.Entry<String, Check> entry : checks.entrySet()) {
      String name = entry.getKey();
      try {
        entry.getValue().run();
        System.out.printf("  PASS  %s%n", name);
      } catch (Throwable t) {
        failed.add(name);
        System.out.printf("  FAIL  %s%n", name);
        System.out.printf("        %s: %s%n", t.getClass().getName(), t.getMessage());
        t.printStackTrace(System.out);
        if (name.equals(JVM_VERSION_CHECK)) {
          // Running on the wrong JVM makes everything below meaningless. Stop rather than emit a wall
          // of passes that a reader could mistake for real coverage.
          System.out.printf("%nAborting: the remaining %d checks would not tell us anything on this JVM.%n",
              checks.size() - 1);
          break;
        }
      }
    }

    System.out.println();
    if (failed.isEmpty()) {
      System.out.printf("All %d checks passed on Java %d.%n", checks.size(), _targetJavaFeatureVersion);
      return 0;
    }
    System.out.printf("%d of %d checks FAILED on Java %d: %s%n", failed.size(), checks.size(),
        _targetJavaFeatureVersion, String.join(", ", failed));
    return 1;
  }

  // ---------------------------------------------------------------------------------------------
  // Checks
  // ---------------------------------------------------------------------------------------------

  /// The point of this harness is that an _old_ JVM runs it. If CI ever hands it the build JDK, every other check would
  /// pass for the wrong reason, so refuse to run.
  private void checkJvmIsAtTargetFeatureVersion() {
    int actual = Runtime.version().feature();
    require(actual == _targetJavaFeatureVersion,
        "expected to be running on a Java %d JVM but this is Java %d (%s); verifying the clients on the build JDK "
            + "would make every other check vacuous", _targetJavaFeatureVersion, actual,
        System.getProperty("java.home"));
  }

  /// Scans the full runtime classpath for bytecode above the target JVM's supported version.
  private void checkClasspathClosureIsLoadable()
      throws IOException {
    String classpath = System.getProperty("java.class.path");
    require(classpath != null && !classpath.isEmpty(), "java.class.path is empty");
    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(classpath, _targetJavaFeatureVersion);

    System.out.printf("        scanned %d archives (%d class files) and %d directories (%d class files); "
            + "major versions: %s%n", result.getArchivesScanned(), result.getClassFilesInArchives(),
        result.getDirectoriesScanned(), result.getClassFilesInDirectories(), result.getMajorVersionHistogram());

    // Vacuity guards. Every one of these has to be able to fail, or a green run means nothing:
    //  - the closure must actually have been resolved, not collapsed to a handful of entries;
    //  - third-party bytecode specifically must have been read. Counting the verifier's own
    //    target/classes towards that would make the guard unfalsifiable, which is why the scanner
    //    keeps the archive and directory counts apart;
    //  - every required Pinot module must be present, so that making one of them optional or
    //    provided shrinks coverage loudly instead of silently.
    require(result.getArchivesScanned() >= MIN_EXPECTED_ARCHIVES,
        "only %d jars on the classpath, expected at least %d -- the runtime closure was not resolved properly",
        result.getArchivesScanned(), MIN_EXPECTED_ARCHIVES);
    require(result.getClassFilesInArchives() >= MIN_EXPECTED_CLASS_FILES_IN_ARCHIVES,
        "only %d class files inspected inside jars, expected at least %d -- the scan is broken, not the closure",
        result.getClassFilesInArchives(), MIN_EXPECTED_CLASS_FILES_IN_ARCHIVES);
    assertRequiredModulesArePresent(result.getArchiveNames());

    ClasspathClosureScanner.assertLoadable(result, _targetJavaFeatureVersion, "client");
  }

  /// Ensures the resolved classpath covers every Pinot artifact exercised by this verifier.
  private static void assertRequiredModulesArePresent(List<String> archiveNames) {
    List<String> missing = new ArrayList<>();
    for (String artifactId : REQUIRED_MODULES) {
      boolean found = false;
      for (String archiveName : archiveNames) {
        // Jar names are <artifactId>-<version>.jar; require the '-' so pinot-spi does not match
        // pinot-spi-something-else.
        if (archiveName.startsWith(artifactId + "-")) {
          found = true;
          break;
        }
      }
      if (!found) {
        missing.add(artifactId);
      }
    }
    require(missing.isEmpty(),
        "these required modules are not on the verified closure, so this job is no longer checking them: %s. "
            + "Either restore the dependency or update REQUIRED_MODULES.", missing);
  }

  /// pinot-common's wire-format schema, both its binary encoding and its JSON encoding.
  private void checkCommonDataSchemaRoundTrip()
      throws IOException {
    DataSchema dataSchema = sampleDataSchema();

    DataSchema fromBytes = DataSchema.fromBytes(ByteBuffer.wrap(dataSchema.toBytes()));
    require(dataSchema.equals(fromBytes), "DataSchema did not survive a binary round trip: %s", fromBytes);

    DataSchema fromJson = JsonUtils.stringToObject(JsonUtils.objectToString(dataSchema), DataSchema.class);
    require(dataSchema.equals(fromJson), "DataSchema did not survive a JSON round trip: %s", fromJson);
    require(dataSchema.getColumnDataType(1) == ColumnDataType.INT, "unexpected column type: %s",
        dataSchema.getColumnDataType(1));
  }

  /// A realistic broker response parsed by pinot-common's own response model.
  private void checkCommonBrokerResponseDeserialization()
      throws IOException {
    BrokerResponseNative response = BrokerResponseNative.fromJsonString(_sampleBrokerResponseJson);
    require(response.getExceptions().isEmpty(), "unexpected exceptions: %s", response.getExceptions());
    require(response.getNumDocsScanned() == 4231, "unexpected numDocsScanned: %d", response.getNumDocsScanned());
    require(response.getTotalDocs() == 97889, "unexpected totalDocs: %d", response.getTotalDocs());
    require(response.getTimeUsedMs() == 37, "unexpected timeUsedMs: %d", response.getTimeUsedMs());
    require("Broker_192.168.1.10_8000".equals(response.getBrokerId()), "unexpected brokerId: %s",
        response.getBrokerId());

    ResultTable resultTable = response.getResultTable();
    require(resultTable != null, "resultTable was dropped during deserialization");
    require(resultTable.getRows().size() == 3, "expected 3 rows, got %d", resultTable.getRows().size());
    require("Hank Aaron".equals(resultTable.getRows().get(0)[0]), "unexpected first cell: %s",
        resultTable.getRows().get(0)[0]);
    require(Arrays.equals(new String[]{
        "playerName", "playerId", "numGames", "battingAverage", "isActive", "teams"
    }, resultTable.getDataSchema().getColumnNames()), "unexpected columns: %s",
        Arrays.toString(resultTable.getDataSchema().getColumnNames()));

    String reserialized = JsonUtils.objectToString(response);
    require(BrokerResponseNative.fromJsonString(reserialized).getNumDocsScanned() == 4231,
        "broker response did not survive a JSON round trip");
  }

  /// Round-trips a result table through every encoder the gRPC client can be asked to decode. The Arrow encoder is the
  /// interesting one: arrow-vector and arrow-memory-netty are the client's most likely source of a Java version floor
  /// bump, and they only fail when actually exercised.
  private void checkCommonResponseEncoders()
      throws IOException {
    ResultTable resultTable = sampleResultTable();
    int numRows = resultTable.getRows().size();
    List<String> encoderTypes = Arrays.asList(ResponseEncoderFactory.getResponseEncoderTypes());
    require(encoderTypes.containsAll(REQUIRED_RESPONSE_ENCODERS),
        "response encoders %s no longer cover %s, so this check would stop exercising them",
        encoderTypes, REQUIRED_RESPONSE_ENCODERS);

    for (String encoderType : encoderTypes) {
      ResponseEncoder encoder = ResponseEncoderFactory.getResponseEncoder(encoderType);
      byte[] encoded = encoder.encodeResultTable(resultTable, 0, numRows);
      require(encoded.length > 0, "%s encoder produced no bytes", encoderType);

      ResultTable decoded = encoder.decodeResultTable(encoded, numRows, resultTable.getDataSchema());
      require(decoded.getRows().size() == numRows, "%s encoder lost rows: %d of %d", encoderType,
          decoded.getRows().size(), numRows);
      assertSampleRowsMatch(encoderType, decoded);
    }
  }

  /// Calcite SQL parsing, which third-party consumers of pinot-common reach through the SQL compiler.
  private void checkCommonCalciteSqlParsing() {
    String sql = "SELECT playerName, SUM(numGames) AS totalGames FROM " + SAMPLE_TABLE_NAME
        + " WHERE battingAverage > 0.3 AND playerName IN ('Hank Aaron', 'Willie Mays') "
        + "GROUP BY playerName ORDER BY totalGames DESC LIMIT 5";

    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    require(SAMPLE_TABLE_NAME.equals(pinotQuery.getDataSource().getTableName()), "unexpected table: %s",
        pinotQuery.getDataSource().getTableName());
    require(pinotQuery.getSelectListSize() == 2, "unexpected select list size: %d", pinotQuery.getSelectListSize());
    require(pinotQuery.getGroupByListSize() == 1, "unexpected group by size: %d", pinotQuery.getGroupByListSize());
    require(pinotQuery.getOrderByListSize() == 1, "unexpected order by size: %d", pinotQuery.getOrderByListSize());
    require(pinotQuery.getLimit() == 5, "unexpected limit: %d", pinotQuery.getLimit());
    require(pinotQuery.getFilterExpression() != null, "filter was dropped");

    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions("SET timeoutMs = 20000; " + sql);
    require("20000".equals(sqlNodeAndOptions.getOptions().get("timeoutMs")), "query option lost: %s",
        sqlNodeAndOptions.getOptions());
  }

  /// pinot-common's ZooKeeper metadata model, which is Helix's `ZNRecord` plus Helix serialization.
  private void checkCommonHelixSegmentMetadata() {
    SegmentZKMetadata metadata = new SegmentZKMetadata(SAMPLE_TABLE_NAME + "_0");
    metadata.setCrc(3141592653L);
    metadata.setTotalDocs(97889);
    metadata.setIndexVersion("v3");
    metadata.setCreationTime(1750000000000L);
    metadata.setStartTime(1749000000000L);
    metadata.setEndTime(1750000000000L);
    metadata.setTimeUnit(TimeUnit.MILLISECONDS);

    ZNRecordSerializer serializer = new ZNRecordSerializer();
    byte[] serialized = serializer.serialize(metadata.toZNRecord());
    require(serialized.length > 0, "ZNRecordSerializer produced no bytes");
    ZNRecord deserialized = (ZNRecord) serializer.deserialize(serialized);
    require(deserialized != null, "ZNRecordSerializer returned null");

    SegmentZKMetadata roundTripped = new SegmentZKMetadata(deserialized);
    require(roundTripped.getCrc() == 3141592653L, "unexpected crc: %d", roundTripped.getCrc());
    require(roundTripped.getTotalDocs() == 97889, "unexpected totalDocs: %d", roundTripped.getTotalDocs());
    require("v3".equals(roundTripped.getIndexVersion()), "unexpected indexVersion: %s",
        roundTripped.getIndexVersion());
    require(roundTripped.getStartTimeMs() == 1749000000000L, "unexpected startTimeMs: %d",
        roundTripped.getStartTimeMs());
    require(roundTripped.getEndTimeMs() == 1750000000000L, "unexpected endTimeMs: %d", roundTripped.getEndTimeMs());
    require(roundTripped.getCreationTime() == 1750000000000L, "unexpected creationTime: %d",
        roundTripped.getCreationTime());

    // The broker list the java client discovers from ZooKeeper is a Helix external view.
    ExternalView externalView = new ExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    externalView.setState(SAMPLE_TABLE_NAME + "_OFFLINE", "Broker_192.168.1.10_8000", "ONLINE");
    require("ONLINE".equals(externalView.getStateMap(SAMPLE_TABLE_NAME + "_OFFLINE").get("Broker_192.168.1.10_8000")),
        "external view state lost: %s", externalView.getStateMap(SAMPLE_TABLE_NAME + "_OFFLINE"));
  }

  /// The gRPC client's decode path end to end: a protobuf `BrokerResponse` carrying a compressed, encoded
  /// payload, unpacked exactly the way `GrpcConnection` unpacks a server response. Covers protobuf, the
  /// compression codecs and the response encoders in one go.
  private void checkCommonGrpcResponseDecoding()
      throws IOException {
    DataSchema dataSchema = sampleResultTable().getDataSchema();
    Broker.BrokerResponse schemaResponse =
        Broker.BrokerResponse.newBuilder().setPayload(ByteString.copyFrom(dataSchema.toBytes())).build();
    require(dataSchema.equals(GrpcUtils.extractSchema(schemaResponse)), "schema lost through the gRPC payload");

    String metadataJson = "{\"requestId\":\"42\",\"numDocsScanned\":4231}";
    Broker.BrokerResponse metadataResponse = Broker.BrokerResponse.newBuilder()
        .setPayload(ByteString.copyFrom(metadataJson.getBytes(StandardCharsets.UTF_8)))
        .putMetadata("rowSize", "3")
        .build();
    require("42".equals(GrpcUtils.extractMetadataJson(metadataResponse).get("requestId").asText()),
        "requestId lost through the gRPC metadata payload");
    ExecutionStats executionStats =
        GrpcUtils.extractExecutionStats(JsonUtils.stringToJsonNode(_sampleBrokerResponseJson));
    require(executionStats.getNumDocsScanned() == 4231, "unexpected numDocsScanned: %d",
        executionStats.getNumDocsScanned());

    ResultTable resultTable = sampleResultTable();
    int numRows = resultTable.getRows().size();
    List<String> compressionTypes = Arrays.asList(CompressionFactory.getCompressionTypes());
    require(compressionTypes.containsAll(REQUIRED_COMPRESSION_CODECS),
        "compression codecs %s no longer cover %s, so the JNI codecs would stop being loaded here",
        compressionTypes, REQUIRED_COMPRESSION_CODECS);

    for (String encoderType : ResponseEncoderFactory.getResponseEncoderTypes()) {
      // Loop-invariant: encoding once per encoder avoids repeating the Arrow allocator setup for
      // every codec.
      byte[] encoded = ResponseEncoderFactory.getResponseEncoder(encoderType)
          .encodeResultTable(resultTable, 0, numRows);
      for (String compressionType : compressionTypes) {
        Compressor compressor = CompressionFactory.getCompressor(compressionType);
        byte[] compressed;
        try {
          compressed = compressor.compress(encoded);
        } catch (Exception e) {
          throw new AssertionError("compressor " + compressionType + " failed on Java "
              + _targetJavaFeatureVersion, e);
        }
        Broker.BrokerResponse dataResponse = Broker.BrokerResponse.newBuilder()
            .setPayload(ByteString.copyFrom(compressed))
            .putMetadata("rowSize", String.valueOf(numRows))
            .putMetadata(CommonConstants.Broker.Grpc.COMPRESSION, compressionType)
            .putMetadata(CommonConstants.Broker.Grpc.ENCODING, encoderType)
            .build();
        ResultTable decoded = GrpcUtils.extractResultTable(dataResponse, dataSchema);
        require(decoded.getRows().size() == numRows, "%s/%s lost rows: %d of %d", encoderType, compressionType,
            decoded.getRows().size(), numRows);
        assertSampleRowsMatch(encoderType + "/" + compressionType, decoded);
      }
    }
  }

  /// Builds the Netty-backed gRPC channel the client uses, without connecting. Exercises grpc-netty's provider lookup
  /// and the pooled direct-buffer allocator, both of which are version sensitive.
  private void checkCommonGrpcChannelConstruction()
      throws IOException {
    // Construction is deliberately outside the try: it builds the Netty channel and the pooled
    // direct-buffer allocator, and either step throwing on an older JVM is the failure we are looking
    // for, so it should propagate as-is -- and there is nothing to close if it never returned.
    BrokerGrpcQueryClient client = new BrokerGrpcQueryClient("localhost", 8010, new GrpcConfig(Map.of()));
    ManagedChannel channel = client.getChannel();
    try {
      require(!channel.isShutdown(), "the gRPC channel was already shut down on creation");
    } finally {
      client.close();
    }
    // close() swallows its own exceptions, so assert the channel state rather than trusting it to throw.
    require(channel.isTerminated(), "the gRPC channel did not terminate on close");
  }

  /// Builds the async-http-client transport the java client queries brokers with, without connecting.
  private void checkJavaClientHttpTransport()
      throws Exception {
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    Properties properties = new Properties();
    properties.setProperty("brokerReadTimeoutMs", "12000");
    properties.setProperty("brokerConnectTimeoutMs", "3000");
    PinotClientTransport<?> transport = factory.withConnectionProperties(properties).buildTransport();
    require(transport != null, "transport was not built");
    transport.close();
  }

  /// The java client's query path, driven against a canned broker response instead of a live cluster.
  private void checkJavaClientQueryExecution() {
    CannedResponseTransport transport = new CannedResponseTransport(_sampleBrokerResponse);
    Connection connection =
        ConnectionFactory.fromHostList(new Properties(), List.of("localhost:8000"), transport);
    try {
      ResultSetGroup resultSetGroup = connection.execute(
          "SELECT playerName, playerId, numGames, battingAverage, isActive, teams FROM " + SAMPLE_TABLE_NAME
              + " LIMIT 3");
      require(transport.getLastQuery() != null && transport.getLastQuery().contains(SAMPLE_TABLE_NAME),
          "the transport never saw the query: %s", transport.getLastQuery());
      require(!resultSetGroup.getBrokerResponse().hasExceptions(), "unexpected exceptions: %s",
          resultSetGroup.getExceptions());
      require(resultSetGroup.getResultSetCount() == 1, "expected 1 result set, got %d",
          resultSetGroup.getResultSetCount());

      ResultSet resultSet = resultSetGroup.getResultSet(0);
      require(resultSet.getRowCount() == 3, "expected 3 rows, got %d", resultSet.getRowCount());
      require(resultSet.getColumnCount() == 6, "expected 6 columns, got %d", resultSet.getColumnCount());
      require("playerName".equals(resultSet.getColumnName(0)), "unexpected column name: %s",
          resultSet.getColumnName(0));
      require("Hank Aaron".equals(resultSet.getString(0, 0)), "unexpected string value: %s",
          resultSet.getString(0, 0));
      require(resultSet.getInt(0, 1) == 1001, "unexpected int value: %d", resultSet.getInt(0, 1));
      require(resultSet.getLong(0, 2) == 3298L, "unexpected long value: %d", resultSet.getLong(0, 2));
      require(Math.abs(resultSet.getDouble(0, 3) - 0.305) < 1e-9, "unexpected double value: %s",
          resultSet.getDouble(0, 3));
      require("Shohei Ohtani".equals(resultSet.getString(2, 0)), "unexpected last row: %s",
          resultSet.getString(2, 0));

      ExecutionStats stats = resultSetGroup.getExecutionStats();
      require(stats.getNumDocsScanned() == 4231, "unexpected numDocsScanned: %d", stats.getNumDocsScanned());
      require(stats.getTotalDocs() == 97889, "unexpected totalDocs: %d", stats.getTotalDocs());
      require(stats.getNumServersQueried() == 2, "unexpected numServersQueried: %d", stats.getNumServersQueried());
      require(stats.getTimeUsedMs() == 37, "unexpected timeUsedMs: %d", stats.getTimeUsedMs());
    } finally {
      connection.close();
    }
    require(transport.isClosed(), "closing the connection did not close the transport");
  }

  /// Parameter binding in the java client's prepared statement.
  private void checkJavaClientPreparedStatement() {
    CannedResponseTransport transport = new CannedResponseTransport(_sampleBrokerResponse);
    Connection connection =
        ConnectionFactory.fromHostList(new Properties(), List.of("localhost:8000"), transport);
    try {
      PreparedStatement statement = connection.prepareStatement(
          "SELECT playerName FROM " + SAMPLE_TABLE_NAME + " WHERE playerName = ? AND playerId > ?");
      statement.setString(0, "Hank Aaron");
      statement.setInt(1, 1000);
      ResultSetGroup resultSetGroup = statement.execute();

      String executedQuery = transport.getLastQuery();
      require(executedQuery != null && executedQuery.contains("'Hank Aaron'"),
          "string parameter was not bound: %s", executedQuery);
      require(executedQuery.contains("1000"), "int parameter was not bound: %s", executedQuery);
      require(!executedQuery.contains("?"), "query still has unbound parameters: %s", executedQuery);
      require(resultSetGroup.getResultSetCount() == 1, "expected 1 result set, got %d",
          resultSetGroup.getResultSetCount());
    } finally {
      connection.close();
    }
  }

  /// The JDBC driver has to be discoverable through [DriverManager] without an explicit `Class.forName`,
  /// which means the `META-INF/services` descriptor and Java's `ServiceLoader` have to work together on
  /// this JVM.
  private void checkJdbcDriverRegistration()
      throws Exception {
    require(getClass().getClassLoader().getResource(JDBC_SERVICE_DESCRIPTOR) != null,
        "%s is missing from the classpath, so DriverManager cannot auto-register the driver",
        JDBC_SERVICE_DESCRIPTOR);

    Driver driver = DriverManager.getDriver("jdbc:pinot://localhost:8000");
    require("org.apache.pinot.client.PinotDriver".equals(driver.getClass().getName()),
        "DriverManager resolved an unexpected driver: %s", driver.getClass().getName());
    require(driver.acceptsURL("jdbc:pinot://localhost:8000"), "the driver rejected a pinot URL");
    require(driver.acceptsURL("jdbc:pinotgrpc://localhost:8010"), "the driver rejected a pinotgrpc URL");
    require(!driver.acceptsURL("jdbc:mysql://localhost:3306/db"), "the driver accepted a non-pinot URL");

    DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo("jdbc:pinot://localhost:8000", new Properties());
    List<String> propertyNames = new ArrayList<>();
    for (DriverPropertyInfo info : propertyInfo) {
      propertyNames.add(info.name);
    }
    // JDBC can report the tenant property in any position.
    require(propertyNames.contains("tenant"), "the driver did not report a tenant property: %s", propertyNames);
    require(driver.getMajorVersion() > 0, "unexpected driver major version: %d", driver.getMajorVersion());
  }

  /// The JDBC result set over a real broker response, including its metadata and type mapping.
  private void checkJdbcResultSet()
      throws Exception {
    // PinotResultSet.fromJson swallows failures and returns an empty result set, so asserting on the
    // row contents is what makes this check meaningful.
    try (PinotResultSet resultSet = PinotResultSet.fromJson(_sampleBrokerResponseJson)) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      require(metaData.getColumnCount() == 6, "expected 6 columns, got %d", metaData.getColumnCount());
      require("playerName".equals(metaData.getColumnName(1)), "unexpected column name: %s",
          metaData.getColumnName(1));
      require(metaData.getColumnType(1) == Types.VARCHAR, "expected VARCHAR for a STRING column, got %d",
          metaData.getColumnType(1));
      require(metaData.getColumnType(4) == Types.DOUBLE, "expected DOUBLE for a DOUBLE column, got %d",
          metaData.getColumnType(4));

      require(resultSet.next(), "the JDBC result set was empty -- PinotResultSet.fromJson swallowed a failure");
      require("Hank Aaron".equals(resultSet.getString(1)), "unexpected string value: %s", resultSet.getString(1));
      require(resultSet.getInt(2) == 1001, "unexpected int value: %d", resultSet.getInt(2));
      require(resultSet.getLong(3) == 3298L, "unexpected long value: %d", resultSet.getLong(3));
      require(Math.abs(resultSet.getDouble(4) - 0.305) < 1e-9, "unexpected double value: %s",
          resultSet.getDouble(4));
      require(new BigDecimal("0.305").compareTo(resultSet.getBigDecimal(4)) == 0, "unexpected big decimal value: %s",
          resultSet.getBigDecimal(4));
      require("Hank Aaron".equals(resultSet.getString("playerName")), "column lookup by name failed: %s",
          resultSet.getString("playerName"));

      int rowCount = 1;
      while (resultSet.next()) {
        rowCount++;
      }
      require(rowCount == 3, "expected 3 rows, got %d", rowCount);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Fixtures and helpers
  // ---------------------------------------------------------------------------------------------

  private static DataSchema sampleDataSchema() {
    return new DataSchema(new String[]{
        "playerName", "playerId", "numGames", "battingAverage", "isActive"
    }, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.BOOLEAN
    });
  }

  /// A result table matching [#sampleDataSchema()] . Values are chosen so that the assertions in
  /// [#assertSampleRowsMatch] are exact rather than approximate.
  private static ResultTable sampleResultTable() {
    return new ResultTable(sampleDataSchema(), List.of(
        new Object[]{"Hank Aaron", 1001, 3298L, 0.305d, false},
        new Object[]{"Willie Mays", 1002, 2992L, 0.301d, false},
        new Object[]{"Shohei Ohtani", 1003, 716L, 0.277d, true}));
  }

  /// Asserts that a decoded result table still carries the values from [#sampleResultTable()]. Compares rendered
  /// values because the encoders are free to pick their own boxed representation for a given [ColumnDataType].
  private static void assertSampleRowsMatch(String label, ResultTable decoded) {
    List<Object[]> expectedRows = sampleResultTable().getRows();
    for (int rowId = 0; rowId < expectedRows.size(); rowId++) {
      Object[] expected = expectedRows.get(rowId);
      Object[] actual = decoded.getRows().get(rowId);
      require(actual.length == expected.length, "%s: row %d has %d columns, expected %d", label, rowId, actual.length,
          expected.length);
      for (int colId = 0; colId < expected.length; colId++) {
        String expectedValue = String.valueOf(expected[colId]);
        String actualValue = String.valueOf(actual[colId]);
        require(expectedValue.equals(actualValue), "%s: row %d column %d is %s, expected %s", label, rowId, colId,
            actualValue, expectedValue);
      }
    }
  }

  private static String readResource(String resource)
      throws IOException {
    try (InputStream inputStream = ClientCompatibilityVerifier.class.getClassLoader()
        .getResourceAsStream(resource)) {
      if (inputStream == null) {
        throw new IOException("Resource not found on the classpath: " + resource);
      }
      return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static void require(boolean condition, String message, Object... args) {
    if (!condition) {
      throw new AssertionError(args.length == 0 ? message : String.format(message, args));
    }
  }
}
