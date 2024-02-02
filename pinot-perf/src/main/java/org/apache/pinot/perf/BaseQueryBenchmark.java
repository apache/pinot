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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.function.DoubleSupplier;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.openjdk.jmh.infra.Blackhole;


public abstract class BaseQueryBenchmark {

  private File _baseDir;
  protected ScenarioBuilder _scenarioBuilder;

  protected void init()
      throws IOException {
    _baseDir = Files.createTempDirectory(getClass().getSimpleName()).toFile();
    _scenarioBuilder = new ScenarioBuilder(_baseDir, getRowsPerSegment(), getSegmentsPerServer(), createTableConfig(),
        createSchema(), createSuppliers());
  }

  protected void tearDown()
      throws IOException {
    if (_baseDir != null) {
      FileUtils.deleteDirectory(_baseDir);
    }
    if (_scenarioBuilder != null) {
      _scenarioBuilder.tearDown();
    }
  }

  protected abstract Schema createSchema();

  protected abstract TableConfig createTableConfig();

  protected abstract int getRowsPerSegment();

  protected abstract int getSegmentsPerServer();

  protected abstract List<IntFunction<Object>> createSuppliers();

  public static class ScenarioBuilder {
    private final File _baseDir;
    private final int _rowsPerSegment;
    private final int _segmentsPerServer;
    private final TableConfig _tableConfig;
    private final Schema _schema;
    private final List<IntFunction<Object>> _suppliers;
    private FluentQueryTest.OnSecondInstance _onSecondInstance;

    public ScenarioBuilder(File baseDir, int rowsPerSegment, int segmentsPerServer,
        TableConfig tableConfig, Schema schema, List<IntFunction<Object>> suppliers) {
      _baseDir = baseDir;
      _rowsPerSegment = rowsPerSegment;
      _segmentsPerServer = segmentsPerServer;
      _tableConfig = tableConfig;
      _schema = schema;
      _suppliers = suppliers;
    }

    public void setup(boolean nullHandling)
        throws IOException {
      FluentQueryTest.OnFirstInstance onFirstInstance =
          FluentQueryTest.withBaseDir(_baseDir)
              .withNullHandling(nullHandling)
              .givenTable(_schema, _tableConfig)
              .onFirstInstance(createSegment());

      for (int i = 1; i < _segmentsPerServer; i++) {
        onFirstInstance = onFirstInstance.andSegment(createSegment());
      }
      FluentQueryTest.OnSecondInstance onSecondInstance = onFirstInstance.andOnSecondInstance(createSegment());
      for (int i = 1; i < _segmentsPerServer; i++) {
        onSecondInstance = onSecondInstance.andSegment(createSegment());
      }

      onSecondInstance.prepareToQuery();

      _onSecondInstance = onSecondInstance;
    }

    public Object[][] createSegment() {
      return IntStream.range(0, _rowsPerSegment)
          .mapToObj(row ->
              _suppliers.stream()
                  .map(supplier -> supplier.apply(row))
                  .toArray())
          .toArray(Object[][]::new);
    }

    public FluentQueryTest.QueryExecuted executeQuery(String query) {
      return _onSecondInstance.whenQuery(query);
    }

    public void consumeQuery(String query, Blackhole bh) {
      bh.consume(executeQuery(query));
    }

    public void tearDown() {
      _onSecondInstance.tearDown();
    }
  }

  public static IntFunction<Object> periodicNulls(int contiguousNullRows, int contiguousNotNullRows,
      boolean startsWithNull, IntFunction<Object> delegate) {
    int period = contiguousNullRows + contiguousNotNullRows;
    return (row) -> {
      int remain = row % period;
      boolean isNull = startsWithNull ? remain < contiguousNullRows : remain >= contiguousNotNullRows;
      return isNull ? null : delegate.apply(row);
    };
  }

  public static IntFunction<Object> longGenerator(String scenario) {
    LongSupplier longSupplier = Distribution.createLongSupplier(42, scenario);
    return (row) -> longSupplier.getAsLong();
  }

  public static IntFunction<Object> doubleGenerator(String scenario) {
    DoubleSupplier doubleSupplier = Distribution.createDoubleSupplier(42, scenario);
    return (row) -> doubleSupplier.getAsDouble();
  }
}
