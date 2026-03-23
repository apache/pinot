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
package org.apache.pinot.core.data.manager.ingest;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link LocalPreparedStore} store, load, list, and cleanup operations.
 */
public class LocalPreparedStoreTest {

  private File _tempDir;
  private LocalPreparedStore _store;

  @BeforeMethod
  public void setUp() {
    _tempDir = new File(FileUtils.getTempDirectory(), "prepared-store-test-" + System.currentTimeMillis());
    _store = new LocalPreparedStore();
    _store.init(_tempDir);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testStoreAndLoad() {
    byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
    _store.store("stmt-1", 0, 0, data);

    byte[] loaded = _store.load("stmt-1", 0, 0);
    assertNotNull(loaded);
    assertEquals(loaded, data);
  }

  @Test
  public void testLoadNonExistent() {
    byte[] result = _store.load("nonexistent", 0, 0);
    assertNull(result);
  }

  @Test
  public void testStoreMultiplePartitions() {
    byte[] data0 = "partition-0".getBytes(StandardCharsets.UTF_8);
    byte[] data1 = "partition-1".getBytes(StandardCharsets.UTF_8);
    byte[] data2 = "partition-2".getBytes(StandardCharsets.UTF_8);

    _store.store("stmt-2", 0, 0, data0);
    _store.store("stmt-2", 1, 0, data1);
    _store.store("stmt-2", 2, 0, data2);

    assertEquals(_store.load("stmt-2", 0, 0), data0);
    assertEquals(_store.load("stmt-2", 1, 0), data1);
    assertEquals(_store.load("stmt-2", 2, 0), data2);
  }

  @Test
  public void testStoreMultipleSequences() {
    byte[] seq0 = "seq-0".getBytes(StandardCharsets.UTF_8);
    byte[] seq1 = "seq-1".getBytes(StandardCharsets.UTF_8);

    _store.store("stmt-3", 0, 0, seq0);
    _store.store("stmt-3", 0, 1, seq1);

    assertEquals(_store.load("stmt-3", 0, 0), seq0);
    assertEquals(_store.load("stmt-3", 0, 1), seq1);
  }

  @Test
  public void testListPreparedStatements() {
    _store.store("stmt-a", 0, 0, "a".getBytes(StandardCharsets.UTF_8));
    _store.store("stmt-b", 0, 0, "b".getBytes(StandardCharsets.UTF_8));
    _store.store("stmt-c", 0, 0, "c".getBytes(StandardCharsets.UTF_8));

    List<String> statements = _store.listPreparedStatements();
    assertEquals(statements.size(), 3);
    assertTrue(statements.contains("stmt-a"));
    assertTrue(statements.contains("stmt-b"));
    assertTrue(statements.contains("stmt-c"));
  }

  @Test
  public void testCleanup() {
    _store.store("stmt-clean", 0, 0, "data".getBytes(StandardCharsets.UTF_8));
    _store.store("stmt-clean", 1, 0, "more data".getBytes(StandardCharsets.UTF_8));

    assertNotNull(_store.load("stmt-clean", 0, 0));
    assertNotNull(_store.load("stmt-clean", 1, 0));

    _store.cleanup("stmt-clean");

    assertNull(_store.load("stmt-clean", 0, 0));
    assertNull(_store.load("stmt-clean", 1, 0));

    List<String> remaining = _store.listPreparedStatements();
    assertTrue(remaining.isEmpty());
  }

  @Test
  public void testCleanupNonExistent() {
    // Should not throw
    _store.cleanup("nonexistent");
  }

  @Test
  public void testListEmptyStore() {
    List<String> statements = _store.listPreparedStatements();
    assertTrue(statements.isEmpty());
  }
}
