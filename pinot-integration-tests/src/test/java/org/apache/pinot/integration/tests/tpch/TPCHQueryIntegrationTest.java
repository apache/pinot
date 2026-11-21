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
package org.apache.pinot.integration.tests.tpch;

import com.google.common.primitives.Doubles;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.utils.JarUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/// Runs the 22 TPC-H queries on the multi-stage engine and compares the results with H2.
///
/// The data is the TPC-H data at scale factor 0.01, generated with `dbgen` (see `README.md` in this package). The
/// data and the queries are in `examples/batch/tpch` of `pinot-tools`, the same files that the TPC-H quickstart uses.
///
/// Each query runs in two forms:
/// - `queries/<n>.sql` is the query text from the TPC-H specification, with the default substitution parameters.
/// - `queries/rewrites/<n>.sql` is the same query, rewritten with a join order and join hints that suit the
///   multi-stage engine (see `queries/README.md`). The hints trust the table layout, so the test uses the layout
///   that the rewrites need: two servers, a full copy of the small tables on each server, and `orders` and
///   `lineitem` partitioned and colocated by order key. When the layout and the hints disagree, the query fails or
///   returns wrong results, and the test fails in both cases.
///
/// H2 runs the specification text, and both forms must return the same rows as H2, in the same order.
public class TPCHQueryIntegrationTest extends BaseClusterIntegrationTest {
  private static final int NUM_TPCH_QUERIES = 22;
  private static final String QUERY_FOLDER = "examples/batch/tpch/queries/";
  private static final int NUM_SERVERS = 2;
  private static final int NUM_ORDERKEY_PARTITIONS = 4;
  private static final String ORDERKEY_PARTITION_FUNCTION = "Murmur";
  private static final Set<String> REPLICATED_TABLES = Set.of("customer", "nation", "part", "region", "supplier");
  private static final Map<String, String> ORDERKEY_PARTITIONED_TABLES =
      Map.of("lineitem", "l_orderkey", "orders", "o_orderkey");
  // Number of rows of each table at scale factor 0.01.
  private static final Map<String, Long> NUM_ROWS = Map.of("customer", 1_500L, "lineitem", 60_175L, "nation", 25L,
      "orders", 15_000L, "part", 2_000L, "partsupp", 8_000L, "region", 5L, "supplier", 100L);
  // The rewrites need the planner to keep the partitioning of a table scan through a local join.
  private static final String REWRITE_QUERY_OPTIONS = "SET inferPartitionHint=true; ";
  // Specification queries that the multi-stage engine cannot run, with the reason. Their rewrites still run.
  private static final Map<Integer, String> SKIPPED_SPECIFICATION_QUERIES = Map.of(21,
      "the plan for the EXISTS and NOT EXISTS subqueries builds a join with more than 10M rows, even at scale factor "
          + "0.01. See queries/rewrites/21.sql");
  // H2 has no indexes by default, and without them the joins in Q3, Q5, Q9 and Q19 take tens of seconds.
  private static final Map<String, List<String>> H2_INDEXED_COLUMNS = Map.of(
      "customer", List.of("c_custkey", "c_nationkey"),
      "lineitem", List.of("l_orderkey", "l_partkey", "l_suppkey"),
      "nation", List.of("n_nationkey", "n_regionkey"),
      "orders", List.of("o_orderkey", "o_custkey"),
      "part", List.of("p_partkey"),
      "partsupp", List.of("ps_partkey", "ps_suppkey"),
      "region", List.of("r_regionkey"),
      "supplier", List.of("s_suppkey", "s_nationkey"));

  // Both forms of a query compare with the same H2 result, so each H2 query runs once.
  private static final double RELATIVE_TOLERANCE = 1e-9;
  private static final double ABSOLUTE_TOLERANCE = 1e-12;
  private final Map<Integer, List<String[]>> _h2Results = new ConcurrentHashMap<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServers(NUM_SERVERS);

    setUpH2Connection();
    for (String tableName : Constants.TPCH_TABLE_NAMES) {
      File resourceFolder = getTableResourceFolder(tableName);
      File dataFile = new File(resourceFolder, "rawdata" + File.separator + tableName + Constants.AVRO_FILE_SUFFIX);
      Assert.assertTrue(dataFile.exists(), "Unable to find the data file: " + dataFile);
      Schema schema = createSchema(new File(resourceFolder, tableName + "_schema.json"));
      addSchema(schema);
      TableConfig tableConfig = createTableConfig(new File(resourceFolder, tableName + "_offline_table_config.json"));
      applyTableLayout(tableConfig);
      addTableConfig(tableConfig);

      File tableSegmentDir = new File(_segmentDir, tableName);
      File tarDir = new File(_tarDir, tableName);
      TestUtils.ensureDirectoriesExistAndEmpty(tableSegmentDir, tarDir);
      String partitionColumn = ORDERKEY_PARTITIONED_TABLES.get(tableName);
      List<File> segmentFiles = partitionColumn == null ? List.of(dataFile)
          : splitByOrderKeyPartition(dataFile, partitionColumn, new File(_tempDir, tableName + "-partitions"));
      ClusterIntegrationTestUtils.buildSegmentsFromAvro(segmentFiles, tableConfig, schema, 0, tableSegmentDir, tarDir);
      uploadSegments(tableName, tarDir);

      ClusterIntegrationTestUtils.setUpH2TableWithAvro(List.of(dataFile), tableName, _h2Connection);
      try (Statement h2Statement = _h2Connection.createStatement()) {
        for (String column : H2_INDEXED_COLUMNS.get(tableName)) {
          h2Statement.execute("CREATE INDEX idx_" + column + " ON " + tableName + "(" + column + ")");
        }
      }
    }
    for (String tableName : Constants.TPCH_TABLE_NAMES) {
      long expectedNumRows = NUM_ROWS.get(tableName);
      TestUtils.waitForCondition(aVoid -> getCurrentCountStarResult(tableName) == expectedNumRows, 60_000L,
          "Failed to load " + expectedNumRows + " rows in table: " + tableName);
    }
  }

  private File getTableResourceFolder(String tableName)
      throws IOException {
    String tableResourceFolder = Constants.getTableResourceFolder(tableName);
    URL resourceUrl = getClass().getClassLoader().getResource(tableResourceFolder);
    Assert.assertNotNull(resourceUrl, "Unable to find resource from: " + tableResourceFolder);
    if (!"jar".equals(resourceUrl.getProtocol())) {
      return new File(resourceUrl.getFile());
    }
    String[] splits = resourceUrl.getFile().split("!");
    File tempUnpackDir = new File(_tempDir.getAbsolutePath() + File.separator + splits[1]);
    TestUtils.ensureDirectoriesExistAndEmpty(tempUnpackDir);
    JarUtils.copyResourcesToDirectory(splits[0], splits[1].substring(1), tempUnpackDir.getAbsolutePath());
    return tempUnpackDir;
  }

  /// Applies the table layout that the hints in `queries/rewrites` need:
  /// - the small tables are replicated to every server, so a join with them can run where the other input is;
  /// - `orders` and `lineitem` are partitioned by order key, and partition `p` of both tables is on the same server,
  ///   so they can be joined and aggregated by order key partition by partition.
  private static void applyTableLayout(TableConfig tableConfig) {
    String tableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
    if (REPLICATED_TABLES.contains(tableName)) {
      tableConfig.getValidationConfig().setReplication(Integer.toString(NUM_SERVERS));
      return;
    }
    String partitionColumn = ORDERKEY_PARTITIONED_TABLES.get(tableName);
    if (partitionColumn == null) {
      return;
    }
    tableConfig.getIndexingConfig().setSegmentPartitionConfig(new SegmentPartitionConfig(
        Map.of(partitionColumn, new ColumnPartitionConfig(ORDERKEY_PARTITION_FUNCTION, NUM_ORDERKEY_PARTITIONS))));
    // One replica group with all servers. Each partition goes to one server. Both tables use the same instance
    // partitions because the constraint config is empty: without it, the instance list is rotated by a hash of the
    // table name, and the two tables would put the same partition on different servers.
    InstanceReplicaGroupPartitionConfig replicaGroupConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, 1, 0, NUM_ORDERKEY_PARTITIONS, 1, false, partitionColumn);
    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig(
        new InstanceTagPoolConfig(TagNameUtils.getOfflineTagForTenant(null), false, 0, null),
        new InstanceConstraintConfig(List.of()), replicaGroupConfig, null, false);
    tableConfig.setInstanceAssignmentConfigMap(
        Map.of(InstancePartitionsType.OFFLINE.name(), instanceAssignmentConfig));
  }

  /// Splits an Avro file into one file for each order key partition, so that each segment holds one partition.
  private static List<File> splitByOrderKeyPartition(File dataFile, String partitionColumn, File outputDir)
      throws IOException {
    TestUtils.ensureDirectoriesExistAndEmpty(outputDir);
    PartitionFunction partitionFunction =
        PartitionFunctionFactory.getPartitionFunction(ORDERKEY_PARTITION_FUNCTION, NUM_ORDERKEY_PARTITIONS, null);
    List<File> partitionFiles = new ArrayList<>(NUM_ORDERKEY_PARTITIONS);
    List<DataFileWriter<GenericRecord>> writers = new ArrayList<>(NUM_ORDERKEY_PARTITIONS);
    try (DataFileReader<GenericRecord> reader = new DataFileReader<>(dataFile, new GenericDatumReader<>())) {
      org.apache.avro.Schema avroSchema = reader.getSchema();
      for (int partition = 0; partition < NUM_ORDERKEY_PARTITIONS; partition++) {
        File partitionFile = new File(outputDir, "partition_" + partition + Constants.AVRO_FILE_SUFFIX);
        DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema));
        writer.create(avroSchema, partitionFile);
        partitionFiles.add(partitionFile);
        writers.add(writer);
      }
      for (GenericRecord record : reader) {
        // The partition function reads the value as a string, which is the same for the Avro long and the Pinot INT.
        int partition = partitionFunction.getPartition(record.get(partitionColumn).toString());
        writers.get(partition).append(record);
      }
    } finally {
      for (DataFileWriter<GenericRecord> writer : writers) {
        writer.close();
      }
    }
    return partitionFiles;
  }

  @Test(dataProvider = "queryNumbers")
  public void testSpecificationQuery(int queryNumber)
      throws Exception {
    String skipReason = SKIPPED_SPECIFICATION_QUERIES.get(queryNumber);
    if (skipReason != null) {
      throw new SkipException("Q" + queryNumber + ": " + skipReason);
    }
    String query = readQuery(QUERY_FOLDER + queryNumber + ".sql");
    compareWithH2(queryNumber, query);
  }

  @Test(dataProvider = "queryNumbers")
  public void testRewrittenQuery(int queryNumber)
      throws Exception {
    String query = readQuery(QUERY_FOLDER + "rewrites/" + queryNumber + ".sql");
    compareWithH2(queryNumber, REWRITE_QUERY_OPTIONS + query);
  }

  private void compareWithH2(int queryNumber, String pinotQuery)
      throws Exception {
    ResultSetGroup pinotResultSetGroup = getPinotConnection().execute(pinotQuery);
    if (CollectionUtils.isNotEmpty(pinotResultSetGroup.getExceptions())) {
      Assert.fail(String.format("TPC-H Q%d raised exception: %s. query: %s", queryNumber,
          pinotResultSetGroup.getExceptions().get(0), pinotQuery));
    }
    org.apache.pinot.client.ResultSet pinotResultSet = pinotResultSetGroup.getResultSet(0);
    List<String[]> h2Rows = getH2Result(queryNumber);
    int numRows = pinotResultSet.getRowCount();
    int numColumns = pinotResultSet.getColumnCount();
    Assert.assertEquals(numRows, h2Rows.size(), "Q" + queryNumber + ": number of rows");
    for (int r = 0; r < numRows; r++) {
      String[] h2Row = h2Rows.get(r);
      Assert.assertEquals(numColumns, h2Row.length, "Q" + queryNumber + ": number of columns");
      for (int c = 0; c < numColumns; c++) {
        String pinotValue = pinotResultSet.getString(r, c);
        if (!valuesMatch(h2Row[c], pinotValue)) {
          Assert.fail(String.format("Q%d: value mismatch at row %d, column %d. H2: %s, Pinot: %s", queryNumber, r, c,
              h2Row[c], pinotValue));
        }
      }
    }
  }

  /// Compares one value of the H2 result with the Pinot value. Numbers match when the relative difference is at most
  /// [#RELATIVE_TOLERANCE], plus [#ABSOLUTE_TOLERANCE] for values near zero. Other values must be equal.
  private static boolean valuesMatch(@Nullable String h2Value, @Nullable String pinotValue) {
    boolean h2Null = h2Value == null || "null".equals(h2Value);
    boolean pinotNull = pinotValue == null || "null".equals(pinotValue);
    if (h2Null || pinotNull) {
      return h2Null && pinotNull;
    }
    Double h2Number = Doubles.tryParse(h2Value);
    Double pinotNumber = Doubles.tryParse(pinotValue);
    if (h2Number == null || pinotNumber == null) {
      return h2Value.equals(pinotValue);
    }
    double tolerance = Math.max(Math.abs(h2Number), Math.abs(pinotNumber)) * RELATIVE_TOLERANCE + ABSOLUTE_TOLERANCE;
    return Math.abs(h2Number - pinotNumber) <= tolerance;
  }

  /// Runs the specification text of the query on H2 and returns the rows. The result is computed once per query.
  private List<String[]> getH2Result(int queryNumber)
      throws Exception {
    List<String[]> rows = _h2Results.get(queryNumber);
    if (rows != null) {
      return rows;
    }
    rows = new ArrayList<>();
    try (Statement h2Statement = _h2Connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY);
        ResultSet h2ResultSet = h2Statement.executeQuery(readQuery(QUERY_FOLDER + queryNumber + ".sql"))) {
      int numColumns = h2ResultSet.getMetaData().getColumnCount();
      while (h2ResultSet.next()) {
        String[] row = new String[numColumns];
        for (int c = 0; c < numColumns; c++) {
          row[c] = h2ResultSet.getString(c + 1);
        }
        rows.add(row);
      }
    }
    _h2Results.put(queryNumber, rows);
    return rows;
  }

  private static String readQuery(String resourcePath)
      throws IOException {
    String query = readOptionalQuery(resourcePath);
    Assert.assertNotNull(query, "Unable to find query file: " + resourcePath);
    return query;
  }

  private static String readOptionalQuery(String resourcePath)
      throws IOException {
    try (InputStream inputStream = TPCHQueryIntegrationTest.class.getClassLoader().getResourceAsStream(resourcePath)) {
      return inputStream == null ? null : IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }
  }

  @Override
  protected boolean useMultiStageQueryEngine() {
    return true;
  }

  @Override
  protected Map<String, String> getExtraQueryProperties() {
    // Increase timeout for this test since it keeps failing in CI.
    Map<String, String> timeoutProperties = new HashMap<>();
    timeoutProperties.put("brokerReadTimeoutMs", "120000");
    timeoutProperties.put("brokerConnectTimeoutMs", "60000");
    timeoutProperties.put("brokerHandshakeTimeoutMs", "60000");
    return timeoutProperties;
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    for (String table : Constants.TPCH_TABLE_NAMES) {
      dropOfflineTable(table);
    }
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @DataProvider(name = "queryNumbers")
  public static Object[][] queryNumbers() {
    Object[][] queryNumbers = new Object[NUM_TPCH_QUERIES][];
    for (int i = 0; i < NUM_TPCH_QUERIES; i++) {
      queryNumbers[i] = new Object[]{i + 1};
    }
    return queryNumbers;
  }
}
