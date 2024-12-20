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

package org.apache.pinot.queries;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.ReadMode;
import org.intellij.lang.annotations.Language;
import org.testng.Assert;


/**
 * A fluent API for testing single-stage queries.
 *
 * Use {@link #withBaseDir(File)} to start a new test.
 *
 * This test framework is intended to be used in a way to write semantically rich tests that are easy to read.
 * They are more useful when small amount of data is needed to test a specific query.
 *
 * By default, this framework creates a single broker and a single server.
 * This should be enough for most tests.
 * But some tests may need to create more than one server.
 * The framework is able to create up to two servers, internally called <em>instances</em>.
 * The DSL force the user to create the first instance before executing a query, but the second instance can be created
 * by calling {@link OnFirstInstance#andOnSecondInstance(Object[]...)}.
 *
 * @see org.apache.pinot.core.query.aggregation.function.CountAggregationFunctionTest
 */
public class FluentQueryTest {

  private final FluentBaseQueriesTest _baseQueriesTest;
  final File _baseDir;
  private final Map<String, String> _extraQueryOptions = new HashMap<>();

  FluentQueryTest(FluentBaseQueriesTest baseQueriesTest, File baseDir) {
    _baseQueriesTest = baseQueriesTest;
    _baseDir = baseDir;
  }

  /**
   * Start a new test with the given base directory.
   *
   * Usually the base directory will be created before every test and destroyed after that using lifecycle testing
   * hooks like {@link org.testng.annotations.BeforeClass} and {@link org.testng.annotations.AfterClass}.
   *
   * Each test will create its own subdirectory in the base directory, so multiple tests may use the same base
   * directory.
   *
   * @param baseDir the base directory for the test. It must exist, be a directory and be writable.
   * @return The fluent API for testing queries, where eventually {@link #givenTable(Schema, TableConfig)} will be
   * called.
   */
  public static FluentQueryTest withBaseDir(File baseDir) {
    Preconditions.checkArgument(baseDir.exists(), "Base directory must exist");
    Preconditions.checkArgument(baseDir.isDirectory(), "Base directory must be a directory");
    Preconditions.checkArgument(baseDir.canWrite(), "Base directory must be writable");
    return new FluentQueryTest(new FluentBaseQueriesTest(), baseDir);
  }

  /**
   * Creates a new test with a temporary directory.
   *
   * @param consumer the test to run. The received FluentQueryTest will use a temporary directory that will be removed
   *                 after the consumer is executed, even if a throwable is thrown.
   */
  public static void test(Consumer<FluentQueryTest> consumer) {
    StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
    try (Closeable test = new Closeable(walker.getCallerClass().getSimpleName())) {
      consumer.accept(test);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Creates a new test with a temporary directory.
   *
   * The returned object is intended to be used in a try-with-resources manner.
   * Its close method will remove the temporary directory.
   */
  public static FluentQueryTest.Closeable open() {
    StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
    try {
      return new FluentQueryTest.Closeable(walker.getCallerClass().getSimpleName());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Sets the given extra query options to the queries that will be executed on this test.
   *
   * Older properties (including null handling) will be removed.
   */
  public FluentQueryTest withExtraQueryOptions(Map<String, String> extraQueryOptions) {
    _extraQueryOptions.clear();
    _extraQueryOptions.putAll(extraQueryOptions);
    return this;
  }

  /**
   * Sets the null handling to the queries that will be executed on this test.
   */
  public FluentQueryTest withNullHandling(boolean enabled) {
    _extraQueryOptions.put("enableNullHandling", Boolean.toString(enabled));
    return this;
  }

  /**
   * Declares a table with the given schema and table configuration.
   *
   * @return a {@link DeclaringTable} object to declare the segments of the table.
   */
  public DeclaringTable givenTable(Schema schema, TableConfig tableConfig) {
    return new DeclaringTable(_baseQueriesTest, tableConfig, schema, _baseDir, _extraQueryOptions);
  }

  public static class Closeable extends FluentQueryTest implements AutoCloseable {

    private Closeable(String testBaseName)
        throws IOException {
      super(new FluentBaseQueriesTest(), Files.createTempDirectory(testBaseName).toFile());
    }

    @Override
    public void close() {
      _baseDir.delete();
    }
  }

  public static class DeclaringTable {
    private final FluentBaseQueriesTest _baseQueriesTest;
    private final TableConfig _tableConfig;
    private final Schema _schema;
    private final File _baseDir;
    private final Map<String, String> _extraQueryOptions;

    DeclaringTable(FluentBaseQueriesTest baseQueriesTest, TableConfig tableConfig, Schema schema, File baseDir,
        Map<String, String> extraQueryOptions) {
      _baseQueriesTest = baseQueriesTest;
      _tableConfig = tableConfig;
      _schema = schema;
      _baseDir = baseDir;
      _extraQueryOptions = extraQueryOptions;
      Preconditions.checkArgument(_schema.getSchemaName() != null, "Schema must have a name");
    }

    /**
     * Moves the fluent DSL to the first instance (aka server).
     * @return
     */
    public OnFirstInstance onFirstInstance() {
      return new OnFirstInstance(_tableConfig, _schema, _baseDir, false, _baseQueriesTest, _extraQueryOptions);
    }

    /**
     * Creates one segment on the first instance (aka server) with the given content.
     *
     * @param content the content of the segment.
     * @see OnFirstInstance#andSegment(String...) to learn more about the content syntax
     */
    public OnFirstInstance onFirstInstance(String... content) {
      return new OnFirstInstance(_tableConfig, _schema, _baseDir, false, _baseQueriesTest, _extraQueryOptions)
          .andSegment(content);
    }

    /**
     * Creates one segment on the first instance (aka server) with the given content.
     * @param content the content of the segment. Each element of the array is a row. Each row is an array of objects
     *                that should be compatible with the table definition.
     */
    public OnFirstInstance onFirstInstance(Object[]... content) {
      return new OnFirstInstance(_tableConfig, _schema, _baseDir, false, _baseQueriesTest, _extraQueryOptions)
          .andSegment(content);
    }
  }

  public static class TableWithSegments {
    protected final TableConfig _tableConfig;
    protected final Schema _schema;
    protected final File _indexDir;
    protected final boolean _onSecondInstance;
    protected final FluentBaseQueriesTest _baseQueriesTest;
    protected final List<FakeSegmentContent> _segmentContents = new ArrayList<>();
    protected final Map<String, String> _extraQueryOptions;

    TableWithSegments(TableConfig tableConfig, Schema schema, File baseDir, boolean onSecondInstance,
        FluentBaseQueriesTest baseQueriesTest, Map<String, String> extraQueryOptions) {
      _extraQueryOptions = extraQueryOptions;
      try {
        _tableConfig = tableConfig;
        _schema = schema;
        _indexDir = Files.createTempDirectory(baseDir.toPath(), schema.getSchemaName()).toFile();
        _onSecondInstance = onSecondInstance;
        _baseQueriesTest = baseQueriesTest;
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }

    TableWithSegments andSegment(String... tableText) {
      _segmentContents.add(new FakeSegmentContent(_schema, tableText));
      return this;
    }

    public TableWithSegments andSegment(Object[]... content) {
      _segmentContents.add(new FakeSegmentContent(content));
      return this;
    }

    protected void processSegments() {
      List<ImmutableSegment> indexSegments = new ArrayList<>(_segmentContents.size());

      try {
        for (int i = 0; i < _segmentContents.size(); i++) {
          FakeSegmentContent segmentContent = _segmentContents.get(i);
          File inputFile = Files.createTempFile(_indexDir.toPath(), "data", ".csv").toFile();
          try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(inputFile), CSVFormat.DEFAULT)) {
            for (List<Object> row : segmentContent) {
              if (row.stream().anyMatch(Objects::isNull)) {
                List<Object> newRow = row.stream().map(o -> o == null ? "null" : o).collect(Collectors.toList());
                csvPrinter.printRecord(newRow);
              } else {
                csvPrinter.printRecord(row);
              }
            }
          } catch (IOException ex) {
            throw new UncheckedIOException(ex);
          }
          String tableName = _schema.getSchemaName();
          SegmentGeneratorConfig config =
              SegmentTestUtils.getSegmentGeneratorConfig(inputFile, FileFormat.CSV, _indexDir, tableName, _tableConfig,
                  _schema);
          CSVRecordReaderConfig csvRecordReaderConfig = new CSVRecordReaderConfig();
          String header = String.join(",", _schema.getPhysicalColumnNames());
          csvRecordReaderConfig.setHeader(header);
          csvRecordReaderConfig.setSkipHeader(false);
          csvRecordReaderConfig.setNullStringValue("null");
          config.setReaderConfig(csvRecordReaderConfig);
          config.setSegmentNamePostfix(Integer.toString(i));
          SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
          driver.init(config);
          driver.build();

          indexSegments.add(ImmutableSegmentLoader.load(new File(_indexDir, driver.getSegmentName()), ReadMode.mmap));
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
      if (_onSecondInstance) {
        _baseQueriesTest._segments2.addAll(indexSegments);
      } else {
        _baseQueriesTest._segments1.addAll(indexSegments);
      }
      _segmentContents.clear();
    }

    /**
     * Executes the given query and returns an object that can be used to assert the results.
     */
    public QueryExecuted whenQuery(@Language("sql") String query) {
      processSegments();
      BrokerResponseNative brokerResponse = _baseQueriesTest.getBrokerResponse(query, _extraQueryOptions);
      return new QueryExecuted(_baseQueriesTest, brokerResponse, _extraQueryOptions);
    }

    /**
     * Executes the given query with null handling enabled and returns an object that can be used to assert the results.
     */
    public QueryExecuted whenQueryWithNullHandlingEnabled(@Language("sql") String query) {
      processSegments();
      _extraQueryOptions.put("enableNullHandling", "true");
      BrokerResponseNative brokerResponse = _baseQueriesTest.getBrokerResponse(query, _extraQueryOptions);
      return new QueryExecuted(_baseQueriesTest, brokerResponse, _extraQueryOptions);
    }

    /**
     * Creates another table.
     *
     * The older tables can still be used.
     */
    public DeclaringTable givenTable(Schema schema, TableConfig tableConfig) {
      processSegments();
      return new DeclaringTable(_baseQueriesTest, tableConfig, schema, _indexDir.getParentFile(), _extraQueryOptions);
    }

    public TableWithSegments prepareToQuery() {
      processSegments();
      return this;
    }

    public void tearDown() {
      _baseQueriesTest.shutdownExecutor();
    }
  }

  public static class OnFirstInstance extends TableWithSegments {
    OnFirstInstance(TableConfig tableConfig, Schema schema, File baseDir, boolean onSecondInstance,
        FluentBaseQueriesTest baseQueriesTest, Map<String, String> extraQueryOptions) {
      super(tableConfig, schema, baseDir, onSecondInstance, baseQueriesTest, extraQueryOptions);
    }

    /**
     * Adds a new segment to the table in this instance.
     * @param content the content of the segment. Each element of the array is a row. Each row is an array of objects
     *                that should be compatible with the table definition.
     */
    public OnFirstInstance andSegment(Object[]... content) {
      _segmentContents.add(new FakeSegmentContent(content));
      return this;
    }

    /**
     * Adds a new segment to the table in this instance.
     *
     * The content is a table in text format. The first row is the header, and the rest of the rows are the data.
     * Each column must be separated by pipes ({@code |}).
     * The header must be the name of the column (as declared in the schema).
     * The order of the columns doesn't have to match the order of the columns in the schema.
     *
     * After the header, each row must have the same number of columns as the header and will contain the data.
     * Each entry in the row must be a valid value for the column type.
     * The rules to parse these values are:
     * <ol>
     *   <li>First, the value will be trimmed</li>
     *   <li>{@code null} will always be treated as null</li>
     *   <li>{@code "null"} will be parsed as
     *   {@link PinotDataType#convert(Object, PinotDataType) PinotDataType.convert("null", type)}</li>
     *   <li>Any other value will be parsed as
     *   {@link PinotDataType#convert(Object, PinotDataType) PinotDataType.convert(value, type)}</li>
     * </ol>
     *
     * @param tableText the content of the segment, as explained above.
     */
    public OnFirstInstance andSegment(String... tableText) {
      super.andSegment(tableText);
      return this;
    }

    /**
     * Moves the fluent DSL to the second instance (aka server).
     */
    public OnSecondInstance andOnSecondInstance() {
      processSegments();
      return new OnSecondInstance(
          _tableConfig, _schema, _indexDir.getParentFile(), !_onSecondInstance, _baseQueriesTest, _extraQueryOptions
      );
    }

    /**
     * Moves the fluent DSL to the second instance (aka server), adding the content as the first segment.
     *
     * @param content the content of the segment. Each element of the array is a row. Each row is an array of objects
     *                that should be compatible with the table definition.
     */
    public OnSecondInstance andOnSecondInstance(Object[]... content) {
      processSegments();
      return new OnSecondInstance(
          _tableConfig, _schema, _indexDir.getParentFile(), !_onSecondInstance, _baseQueriesTest, _extraQueryOptions)
          .andSegment(content);
    }

    /**
     * Creates one segment on a second instance (aka server).
     *
     * @param content the content of the segment.
     * @see OnFirstInstance#andSegment(String...) to learn more about the content syntax
     */
    public OnSecondInstance andOnSecondInstance(String... content) {
      processSegments();
      return new OnSecondInstance(
          _tableConfig, _schema, _indexDir.getParentFile(), !_onSecondInstance, _baseQueriesTest, _extraQueryOptions)
          .andSegment(content);
    }

    public OnFirstInstance prepareToQuery() {
      super.prepareToQuery();
      return this;
    }
  }

  public static class OnSecondInstance extends TableWithSegments {
    OnSecondInstance(TableConfig tableConfig, Schema schema, File baseDir, boolean onSecondInstance,
        FluentBaseQueriesTest baseQueriesTest, Map<String, String> extraQueryOptions) {
      super(tableConfig, schema, baseDir, onSecondInstance, baseQueriesTest, extraQueryOptions);
    }

    /**
     * Adds a new segment to the table in this instance.
     * @param content the content of the segment. Each element of the array is a row. Each row is an array of objects
     *                that should be compatible with the table definition.
     */
    public OnSecondInstance andSegment(Object[]... content) {
      _segmentContents.add(new FakeSegmentContent(content));
      return this;
    }

    /**
     * Adds a new segment to the table in this instance.
     * @param tableText the content of the segment.
     * @see OnFirstInstance#andSegment(String...) to learn more about the
     */
    public OnSecondInstance andSegment(String... tableText) {
      super.andSegment(tableText);
      return this;
    }

    public OnSecondInstance prepareToQuery() {
      super.prepareToQuery();
      return this;
    }
  }

  public static class QueryExecuted {
    private final FluentBaseQueriesTest _baseQueriesTest;
    private final BrokerResponse _brokerResponse;
    private final Map<String, String> _extraQueryOptions;

    public QueryExecuted(FluentBaseQueriesTest baseQueriesTest, BrokerResponse brokerResponse,
        Map<String, String> extraQueryOptions) {
      _baseQueriesTest = baseQueriesTest;
      _brokerResponse = brokerResponse;
      _extraQueryOptions = extraQueryOptions;
    }

    /**
     * Asserts that the result of the query is the given table.
     *
     * The table is a text table. The first row is the header, and the rest of the rows are the data.
     * Each column must be separated by pipes ({@code |}).
     * The header must be a valid column type (as defined by {@link PinotDataType}, although it will be trimmed and
     * uppercased).
     *
     * After the header, each row must have the same number of columns as the header and will contain the data.
     * Each entry in the row must be a valid value for the column type.
     * The rules to parse these values are:
     * <ol>
     *   <li>First, the value will be trimmed</li>
     *   <li>{@code null} will always be treated as null</li>
     *   <li>{@code "null"} will be parsed as
     *   {@link PinotDataType#convert(Object, PinotDataType) PinotDataType.convert("null", type)}</li>
     *   <li>Any other value will be parsed as
     *   {@link PinotDataType#convert(Object, PinotDataType) PinotDataType.convert(value, type)}</li>
     * </ol>
     */
    public QueryExecuted thenResultIs(String... tableText) {
      Object[][] rows = tableAsRows(
          headerCells -> Arrays.stream(headerCells)
              .map(String::trim)
              .map(txt -> txt.toUpperCase(Locale.US))
              .map(PinotDataType::valueOf)
              .collect(Collectors.toList()),
          tableText
      );
      thenResultIs(rows);

      return this;
    }

    public QueryExecuted thenResultIsException(String messagePattern) {
      return thenResultIsException(messagePattern, -1);
    }

    public QueryExecuted thenResultIsException(String messagePattern, int exceptionsNumber) {
      if (_brokerResponse.getExceptionsSize() == 0) {
        Assert.fail("Expected exception " + messagePattern + " but query didn't throw one.");
      }

      List<QueryProcessingException> exceptions = _brokerResponse.getExceptions();

      Pattern pattern = Pattern.compile(messagePattern);

      for (int i = 0, n = exceptions.size(); i < n; i++) {
        if (!pattern.matcher(exceptions.get(i).getMessage()).find()) {
          Assert.fail(
              "Exception number: " + i + " doesn't match pattern: "
                  + messagePattern + "\n exception: " + exceptions.get(i).getMessage());
        }
      }

      if (exceptionsNumber > 0 && exceptionsNumber != _brokerResponse.getExceptionsSize()) {
        Assert.fail("Expected " + exceptionsNumber + " exceptions but found " + _brokerResponse.getExceptionsSize());
      }

      return this;
    }

    /**
     * Asserts that the result of the query is the given table.
     */
    public QueryExecuted thenResultIs(Object[]... expectedResult) {
      if (_brokerResponse.getExceptionsSize() > 0) {
        Assert.fail("Query failed with " + _brokerResponse.getExceptions());
      }

      List<Object[]> actualRows = _brokerResponse.getResultTable().getRows();
      int rowsToAnalyze = Math.min(actualRows.size(), expectedResult.length);
      for (int i = 0; i < rowsToAnalyze; i++) {
        Object[] actualRow = actualRows.get(i);
        Object[] expectedRow = expectedResult[i];
        int colsToAnalyze = Math.min(actualRow.length, expectedRow.length);
        for (int j = 0; j < colsToAnalyze; j++) {
          Object actualCell = actualRow[j];
          Object expectedCell = expectedRow[j];
          if (actualCell != null && expectedCell != null) {
            Assert.assertEquals(actualCell.getClass(), expectedCell.getClass(), "On row " + i + " and column " + j);
          }
          if (expectedCell == null) {
            Assert.assertNull(actualCell, "On row " + i + " and column " + j + ". "
                + "Actual value is '" + actualCell + "', which is not null");
          } else if (actualCell == null) {
            Assert.fail("On row " + i + " and column " + j + ". Actual value is null when expecting not null "
                + "value '" + expectedCell + "'");
          } else {
            Assert.assertEquals(actualCell, expectedCell, "On row " + i + " and column " + j);
          }
        }
        Assert.assertEquals(actualRow.length, expectedRow.length, "Unexpected number of columns on row " + i);
      }
      Assert.assertEquals(actualRows.size(), expectedResult.length, "Unexpected number of rows");
      return this;
    }

    /**
     * Sets the given extra query options to the queries that will be executed on this test.
     *
     * Older properties (including null handling) will be removed.
     */
    public QueryExecuted withExtraQueryOptions(Map<String, String> extraQueryOptions) {
      _extraQueryOptions.clear();
      _extraQueryOptions.putAll(extraQueryOptions);
      return this;
    }

    /**
     * Sets the null handling to the queries that will be executed on this test.
     *
     * <strong>Important:</strong> This change will only affect new queries.
     */
    public QueryExecuted withNullHandling(boolean enabled) {
      _extraQueryOptions.put("enableNullHandling", Boolean.toString(enabled));
      return this;
    }

    /**
     * Executes the given query and returns an object that can be used to assert the results.
     *
     * The tables and segments already created can still be used.
     */
    public QueryExecuted whenQuery(@Language("sql") String query) {
      BrokerResponseNative brokerResponse = _baseQueriesTest.getBrokerResponse(query, _extraQueryOptions);
      return new QueryExecuted(_baseQueriesTest, brokerResponse, _extraQueryOptions);
    }

    /**
     * Executes the given query with null handling enabled and returns an object that can be used to assert the results.
     */
    public QueryExecuted whenQueryWithNullHandlingEnabled(@Language("sql") String query) {
      _extraQueryOptions.put("enableNullHandling", "true");
      BrokerResponseNative brokerResponse = _baseQueriesTest.getBrokerResponse(query, _extraQueryOptions);
      return new QueryExecuted(_baseQueriesTest, brokerResponse, _extraQueryOptions);
    }
  }

  public static Object[][] tableAsRows(Function<String[], List<PinotDataType>> extractDataTypes, String... tableText) {
    String header = tableText[0];
    String[] headerCells = header.split("\\|");

    List<PinotDataType> dataTypes = extractDataTypes.apply(headerCells);

    for (int i = 0; i < dataTypes.size(); i++) {
      PinotDataType dataType = dataTypes.get(i);
      if (!dataType.isSingleValue()) {
        throw new IllegalArgumentException(
            "Multi value columns are not supported and the " + i + "th column is of type " + dataType
                + " which is multivalued");
      }
    }

    Object[][] rows = new Object[tableText.length - 1][];
    for (int i = 1; i < tableText.length; i++) {
      String[] rawCells = tableText[i].split("\\|");
      Object[] convertedRow = new Object[dataTypes.size()];
      for (int col = 0; col < rawCells.length; col++) {
        String rawCell = rawCells[col].trim();
        Object converted;
        if (rawCell.equalsIgnoreCase("null")) {
          converted = null;
        } else if (rawCell.equalsIgnoreCase("\"null\"")) {
          converted = dataTypes.get(col).convert("null", PinotDataType.STRING);
        } else {
          converted = dataTypes.get(col).convert(rawCell, PinotDataType.STRING);
        }
        convertedRow[col] = converted;
      }
      rows[i - 1] = convertedRow;
    }
    return rows;
  }

  public static class FakeSegmentContent extends ArrayList<List<Object>> {

    public FakeSegmentContent(Schema schema, String... tableText) {
      super(tableText.length - 1);

      Object[][] rows = FluentQueryTest.tableAsRows(
          headerCells -> {
            List<PinotDataType> dataTypes = new ArrayList<>();
            for (String headerCell : headerCells) {
              String columnName = headerCell.trim();
              FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
              if (fieldSpec.isVirtualColumn()) {
                throw new IllegalArgumentException("Virtual columns like " + columnName + " cannot be set here");
              }
              if (!fieldSpec.isSingleValueField()) {
                throw new IllegalArgumentException(
                    "Multi valued columns like " + columnName + " cannot be set as text");
              }
              dataTypes.add(PinotDataType.getPinotDataTypeForIngestion(fieldSpec));
            }
            return dataTypes;
          },
          tableText);

      for (Object[] row : rows) {
        add(Arrays.asList(row));
      }
    }

    public FakeSegmentContent(Object[]... rows) {
      super(rows.length);
      for (Object[] row : rows) {
        add(Arrays.asList(row));
      }
    }
  }

  protected static class FluentBaseQueriesTest extends BaseQueriesTest {
    List<IndexSegment> _segments1 = new ArrayList<>();
    List<IndexSegment> _segments2 = new ArrayList<>();

    @Override
    protected String getFilter() {
      return "";
    }

    @Override
    protected IndexSegment getIndexSegment() {
      return _segments1.get(0);
    }

    @Override
    protected List<IndexSegment> getIndexSegments() {
      if (_segments2.isEmpty()) {
        return _segments1;
      }
      ArrayList<IndexSegment> segments = new ArrayList<>(_segments1.size() + _segments2.size());
      segments.addAll(_segments1);
      segments.addAll(_segments2);
      return segments;
    }

    @Override
    protected List<List<IndexSegment>> getDistinctInstances() {
      if (_segments2.isEmpty()) {
        return super.getDistinctInstances();
      }
      return Lists.newArrayList(_segments1, _segments2);
    }
  }
}
