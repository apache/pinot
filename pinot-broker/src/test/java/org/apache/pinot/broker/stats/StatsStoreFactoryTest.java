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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.pinot.query.planner.spi.stats.StatsStore;
import org.apache.pinot.query.planner.spi.stats.StatsStoreException;
import org.apache.pinot.query.planner.spi.stats.StatsStoreProvider;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the mapping from the configured `pinot.broker.stats.store` NAME to a [StatsStore],
/// resolved over the registered [org.apache.pinot.query.planner.spi.stats.StatsStoreProvider]s.
public class StatsStoreFactoryTest {

  private static final Map<String, String> PROPS =
      Map.of(SqliteStatsStoreProvider.DIR_KEY,
          System.getProperty("java.io.tmpdir") + "/stats-store-factory-test");

  @DataProvider(name = "sqliteNames")
  public Object[][] sqliteNames() {
    // null and blank fall back to the default, which is sqlite; matching is case-insensitive.
    return new Object[][]{{null}, {""}, {"   "}, {"sqlite"}, {"SQLite"}, {"SQLITE"}};
  }

  @Test(dataProvider = "sqliteNames")
  public void testSqliteSelected(String configured)
      throws Exception {
    try (StatsStore store = StatsStoreFactory.create(configured, PROPS)) {
      assertTrue(store instanceof SqliteStatsStore, "Expected SQLite store for value: " + configured);
    }
  }

  @DataProvider(name = "memoryNames")
  public Object[][] memoryNames() {
    return new Object[][]{{"memory"}, {"MEMORY"}, {"Memory"}};
  }

  @Test(dataProvider = "memoryNames")
  public void testMemorySelected(String configured)
      throws Exception {
    try (StatsStore store = StatsStoreFactory.create(configured, PROPS)) {
      assertTrue(store instanceof InMemoryStatsStore, "Expected in-memory store for value: " + configured);
    }
  }

  /// A misconfigured name must fail loudly rather than silently disabling statistics an operator
  /// explicitly enabled, and must say what names actually exist.
  @Test
  public void testUnknownNameFailsFastAndListsWhatExists() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> StatsStoreFactory.create("not-a-store", PROPS));
    assertTrue(e.getMessage().contains("sqlite"), "Message should list available names: " + e.getMessage());
    assertTrue(e.getMessage().contains("memory"), "Message should list available names: " + e.getMessage());
  }

  /// A class name is not a valid selector: stores are chosen by the name a provider declares, so
  /// that renaming or moving an implementation cannot invalidate an operator's configuration.
  @Test
  public void testClassNameIsNotAccepted() {
    assertThrows(IllegalArgumentException.class,
        () -> StatsStoreFactory.create(InMemoryStatsStore.class.getName(), PROPS));
  }

  /// A provider that cannot build a store from the given configuration must degrade statistics,
  /// not stop the broker: the two failure modes are deliberately different exception types, and
  /// getting them the wrong way round inverts the operator-visible outcome.
  @Test
  public void testProviderFailureDegradesRatherThanFailingStartup() {
    StatsStoreException e = expectThrows(StatsStoreException.class,
        () -> StatsStoreFactory.create("sqlite", Map.of()));
    assertTrue(e.getMessage().contains("directory"), "Unexpected message: " + e.getMessage());
  }

  @Test
  public void testProviderRuntimeExceptionIsWrapped() {
    StatsStoreProvider exploding = new StubProvider("exploding", () -> {
      throw new IllegalStateException("boom");
    });
    StatsStoreException e = expectThrows(StatsStoreException.class,
        () -> StatsStoreFactory.createFrom(exploding, "exploding", Map.of()));
    assertTrue(e.getMessage().contains("exploding"), "Message should name the provider: " + e.getMessage());
    assertTrue(e.getCause() instanceof IllegalStateException);
  }

  @Test
  public void testProviderStatsStoreExceptionPassesThroughUnwrapped() {
    StatsStoreException thrown = new StatsStoreException("no directory");
    StatsStoreProvider failing = new StubProvider("failing", () -> {
      throw thrown;
    });
    assertSame(expectThrows(StatsStoreException.class,
        () -> StatsStoreFactory.createFrom(failing, "failing", Map.of())), thrown);
  }

  /// Two providers answering to one name would make configuration silently pick one of them.
  @Test
  public void testDuplicateNamesAreRejected() {
    StatsStoreProvider first = new StubProvider("dup", () -> null);
    StatsStoreProvider second = new OtherStubProvider("DUP");
    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> StatsStoreFactory.collectProviders(List.of(first, second), new LinkedHashSet<>(), new TreeMap<>()));
    assertTrue(e.getMessage().contains("dup"), "Unexpected message: " + e.getMessage());
  }

  /// The same provider class reached through two classloaders is not a name collision: a fat jar
  /// and a plugin realm can both expose one META-INF/services entry.
  @Test
  public void testSameProviderClassSeenTwiceIsNotADuplicate() {
    Map<String, StatsStoreProvider> providers = new TreeMap<>();
    Set<String> seen = new LinkedHashSet<>();
    StatsStoreFactory.collectProviders(List.of(new StubProvider("once", () -> null)), seen, providers);
    StatsStoreFactory.collectProviders(List.of(new StubProvider("once", () -> null)), seen, providers);
    assertEquals(providers.size(), 1);
  }

  private interface StoreSupplier {
    StatsStore get()
        throws StatsStoreException;
  }

  private static class StubProvider implements StatsStoreProvider {
    private final String _name;
    private final StoreSupplier _supplier;

    StubProvider(String name, StoreSupplier supplier) {
      _name = name;
      _supplier = supplier;
    }

    @Override
    public String getName() {
      return _name;
    }

    @Override
    public StatsStore create(Map<String, String> properties)
        throws StatsStoreException {
      return _supplier.get();
    }
  }

  /// A distinct class, so the dedup-by-class-name step cannot mask a genuine name collision.
  private static class OtherStubProvider implements StatsStoreProvider {
    private final String _name;

    OtherStubProvider(String name) {
      _name = name;
    }

    @Override
    public String getName() {
      return _name;
    }

    @Override
    public StatsStore create(Map<String, String> properties) {
      return null;
    }
  }
}
