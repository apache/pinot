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
package org.apache.pinot.server.starter.helix;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Server.INSTANCE_DATA_DIR;
import static org.apache.pinot.spi.utils.CommonConstants.Server.INSTANCE_DATA_DIR_FAIL_ON_MISSING;
import static org.apache.pinot.spi.utils.CommonConstants.Server.INSTANCE_ID;
import static org.apache.pinot.spi.utils.CommonConstants.Server.INSTANCE_SEGMENT_TAR_DIR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


public class HelixInstanceDataManagerTest {

  private HelixInstanceDataManager _helixInstanceDataManager;
  private File _testDir;
  private File _testInstanceDataDir;
  private File _testInstanceSegmentTarDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    _testDir = Files.createTempDirectory("pinot-instance-data-manager-test").toFile();
    _testInstanceDataDir = new File(_testDir, "data");
    _testInstanceSegmentTarDir = new File(_testDir, "segmentTar");
    _helixInstanceDataManager = createInstanceDataManager(new PinotConfiguration(Map.of(INSTANCE_ID, "testInstance",
        INSTANCE_DATA_DIR, _testInstanceDataDir.toString(), INSTANCE_SEGMENT_TAR_DIR,
        _testInstanceSegmentTarDir.toString())));
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    FileUtils.deleteQuietly(_testDir);
  }

  private static HelixInstanceDataManager createInstanceDataManager(PinotConfiguration config)
      throws Exception {
    HelixInstanceDataManager manager = new HelixInstanceDataManager();
    FieldUtils.writeField(manager, "_instanceDataManagerConfig", new HelixInstanceDataManagerConfig(config), true);
    return manager;
  }

  @Test
  public void testInitInstanceDataDirCreatesDirectory() {
    if (_testInstanceDataDir.exists()) {
      FileUtils.deleteQuietly(_testInstanceDataDir);
    }

    _helixInstanceDataManager.initInstanceDataDir();

    assertTrue(_testInstanceDataDir.exists(), "Instance data directory should be created.");
  }

  @Test
  public void testInitInstanceDataDirFailsOnMissingDirectory()
      throws Exception {
    File tempDir = Files.createTempDirectory("pinot-data-dir-test").toFile();
    File dataDir = new File(tempDir, "disk/data");
    try {
      HelixInstanceDataManager manager = createInstanceDataManager(new PinotConfiguration(Map.of(INSTANCE_ID,
          "testInstance", INSTANCE_DATA_DIR, dataDir.toString(), INSTANCE_DATA_DIR_FAIL_ON_MISSING, true)));
      IllegalStateException exception = expectThrows(IllegalStateException.class, manager::initInstanceDataDir);
      assertTrue(exception.getMessage().contains(dataDir.toString()));
      assertTrue(exception.getMessage().contains("pinot.server.instance.dataDir.failOnMissing=true"));
      assertFalse(dataDir.getParentFile().exists(), "Missing data directory must not be created.");

      assertTrue(dataDir.mkdirs(), "Failed to prepare data directory.");
      manager.initInstanceDataDir();
      assertTrue(dataDir.isDirectory());
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  @Test
  public void testInitInstanceDataDirCleansUpTempDirs() {
    File tableDir = new File(_testInstanceDataDir, "tableA_OFFLINE");
    assertTrue(tableDir.mkdirs(), "Failed to create temp directory.");

    _helixInstanceDataManager.initInstanceDataDir();

    assertFalse(tableDir.exists(), "Temporary directory should be deleted.");
  }

  @Test
  public void testInitInstanceDataDirThrowsIfNotWritable() {
    if (!_testInstanceDataDir.exists()) {
      assertTrue(_testInstanceDataDir.mkdirs(), "Failed to create test directory.");
    }
    _testInstanceDataDir.setWritable(false);

    try {
      _helixInstanceDataManager.initInstanceDataDir();
      fail("Expected IllegalStateException due to non-writable directory.");
    } catch (IllegalStateException e) {
      // Expected exception
    } finally {
      _testInstanceDataDir.setWritable(true);
    }
  }

  @Test
  public void testDirectoryWritabilityCheckDoesNotLeaveProbeFile()
      throws IOException {
    if (!_testInstanceDataDir.exists()) {
      assertTrue(_testInstanceDataDir.mkdirs(), "Failed to create test directory.");
    } else {
      FileUtils.cleanDirectory(_testInstanceDataDir);
    }

    HelixInstanceDataManager.ensureDirectoryWritable(_testInstanceDataDir, "instance data dir");

    File[] files = _testInstanceDataDir.listFiles();
    assertNotNull(files, "Expected test directory to remain listable after writability check.");
    assertEquals(files.length, 0, "Writability check should not leave probe files behind.");
  }

  @Test
  public void testInitInstanceSegmentTarDirCreatesDirectory() {
    if (_testInstanceSegmentTarDir.exists()) {
      FileUtils.deleteQuietly(_testInstanceSegmentTarDir);
    }

    _helixInstanceDataManager.initInstanceSegmentTarDir();

    assertTrue(_testInstanceSegmentTarDir.exists(), "Instance segment tar directory should be created.");
  }

  @Test
  public void testInitInstanceSegmentTarDirThrowsIfNotWritable() {
    if (!_testInstanceSegmentTarDir.exists()) {
      assertTrue(_testInstanceSegmentTarDir.mkdirs(), "Failed to create test directory.");
    }
    _testInstanceSegmentTarDir.setWritable(false);

    try {
      _helixInstanceDataManager.initInstanceSegmentTarDir();
      fail("Expected IllegalStateException due to non-writable directory.");
    } catch (IllegalStateException e) {
      // Expected exception
    } finally {
      _testInstanceSegmentTarDir.setWritable(true);
    }
  }
}
