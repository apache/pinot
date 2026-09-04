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
package org.apache.pinot.broker.stats;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.query.planner.spi.stats.StatsStore;
import org.apache.pinot.query.planner.spi.stats.StatsStoreException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests how [SqliteStatsStoreProvider] turns `pinot.broker.stats.*` properties into a configured
/// store, with particular attention to how it treats values an operator got wrong.
public class SqliteStatsStoreProviderTest {

  private final SqliteStatsStoreProvider _provider = new SqliteStatsStoreProvider();
  private Path _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = Files.createTempDirectory("sqlite-provider-test-");
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    if (_tempDir != null && Files.exists(_tempDir)) {
      try (var stream = Files.walk(_tempDir)) {
        stream.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
          try {
            Files.deleteIfExists(p);
          } catch (IOException e) {
            // best effort
          }
        });
      }
    }
  }

  private Map<String, String> props(String... keyValues) {
    Map<String, String> properties = new HashMap<>();
    properties.put(SqliteStatsStoreProvider.DIR_KEY, _tempDir.toString());
    for (int i = 0; i < keyValues.length; i += 2) {
      properties.put(keyValues[i], keyValues[i + 1]);
    }
    return properties;
  }

  @Test
  public void testDefaultsWhenPoolPropertiesAreAbsent()
      throws Exception {
    try (StatsStore store = _provider.create(props())) {
      store.init();
      assertTrue(store instanceof SqliteStatsStore);
    }
  }

  /// A configured pool must actually reach the store, not just parse: sizing the pool at one and
  /// draining it proves the value was applied rather than silently replaced by the default.
  @Test
  public void testConfiguredPoolSizeReachesTheStore()
      throws Exception {
    try (StatsStore store = _provider.create(
        props(SqliteStatsStoreProvider.READ_POOL_SIZE_KEY, "1",
            SqliteStatsStoreProvider.READ_TIMEOUT_MS_KEY, "20"))) {
      store.init();
      java.lang.reflect.Field field = SqliteStatsStore.class.getDeclaredField("_readPool");
      field.setAccessible(true);
      java.util.concurrent.BlockingQueue<?> pool = (java.util.concurrent.BlockingQueue<?>) field.get(store);
      assertEquals(pool.size(), 1, "Configured pool size did not reach the store");
    }
  }

  @DataProvider(name = "unusableValues")
  public Object[][] unusableValues() {
    return new Object[][]{
        {"0"}, {"-1"}, {"abc"}, {"4.5"}, {"1e3"}
    };
  }

  /// A present-but-unusable value is a typo in an operator's config. Failing is what tells them so;
  /// silently falling back to the default would leave them tuning a knob that does nothing.
  @Test(dataProvider = "unusableValues")
  public void testUnusablePoolSizeIsRejected(String value) {
    StatsStoreException e = expectThrows(StatsStoreException.class,
        () -> _provider.create(props(SqliteStatsStoreProvider.READ_POOL_SIZE_KEY, value)));
    assertTrue(e.getMessage().contains(SqliteStatsStoreProvider.READ_POOL_SIZE_KEY),
        "Message should name the offending key: " + e.getMessage());
  }

  @Test(dataProvider = "unusableValues")
  public void testUnusableTimeoutIsRejected(String value) {
    StatsStoreException e = expectThrows(StatsStoreException.class,
        () -> _provider.create(props(SqliteStatsStoreProvider.READ_TIMEOUT_MS_KEY, value)));
    assertTrue(e.getMessage().contains(SqliteStatsStoreProvider.READ_TIMEOUT_MS_KEY),
        "Message should name the offending key: " + e.getMessage());
  }

  /// Each pooled reader holds an open connection, so an accidental extra digit must be caught
  /// rather than allowed to exhaust file handles at broker startup.
  @Test
  public void testAbsurdlyLargePoolSizeIsRejected() {
    StatsStoreException e = expectThrows(StatsStoreException.class,
        () -> _provider.create(props(SqliteStatsStoreProvider.READ_POOL_SIZE_KEY, "100000")));
    assertTrue(e.getMessage().contains("at most"), "Unexpected message: " + e.getMessage());
  }

  /// Whitespace around a value is an artefact of how the property was written, not an error.
  @Test
  public void testValuesAreTrimmed()
      throws Exception {
    try (StatsStore store = _provider.create(
        props(SqliteStatsStoreProvider.READ_POOL_SIZE_KEY, "  3  "))) {
      store.init();
      java.lang.reflect.Field field = SqliteStatsStore.class.getDeclaredField("_readPool");
      field.setAccessible(true);
      java.util.concurrent.BlockingQueue<?> pool = (java.util.concurrent.BlockingQueue<?>) field.get(store);
      assertEquals(pool.size(), 3);
    }
  }
}
