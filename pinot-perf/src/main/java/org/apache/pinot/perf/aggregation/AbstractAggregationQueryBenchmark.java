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
package org.apache.pinot.perf.aggregation;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Base class for aggregation query benchmarks.
 */
public abstract class AbstractAggregationQueryBenchmark {

  private File _baseDir;
  private FluentQueryTest.OnSecondInstance _onSecondInstance;

  protected void init(boolean nullHandlingEnabled) throws IOException {
    _baseDir = Files.createTempDirectory(getClass().getSimpleName()).toFile();
    TableConfig tableConfig = createTableConfig();
    Schema schema = createSchema();
    List<List<Object[][]>> segmentsPerServer = createSegmentsPerServer();

    FluentQueryTest.OnFirstInstance onFirstInstance =
        FluentQueryTest.withBaseDir(_baseDir)
            .withNullHandling(nullHandlingEnabled)
            .givenTable(schema, tableConfig)
            .onFirstInstance();

    List<Object[][]> segmentsOnFirstServer = segmentsPerServer.get(0);
    for (Object[][] segment : segmentsOnFirstServer) {
      onFirstInstance.andSegment(segment);
    }

    FluentQueryTest.OnSecondInstance onSecondInstance = onFirstInstance.andOnSecondInstance();
    List<Object[][]> segmentsOnSecondServer = segmentsPerServer.get(1);
    for (Object[][] segment : segmentsOnSecondServer) {
      onSecondInstance.andSegment(segment);
    }
    onSecondInstance.prepareToQuery();
    _onSecondInstance = onSecondInstance;
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (_baseDir != null) {
      FileUtils.deleteDirectory(_baseDir);
    }
    _onSecondInstance.tearDown();
  }

  protected void executeQuery(String query, Blackhole bh) {
    bh.consume(_onSecondInstance.whenQuery(query));
  }

  protected abstract Schema createSchema();

  protected abstract TableConfig createTableConfig();

  /**
   * Returns a list of segments to be created on the servers. The first list is the list of segments to be
   * created on the first server and the second list is the segments to be created on the second server.
   */
  protected abstract List<List<Object[][]>> createSegmentsPerServer();
}
