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
import org.apache.commons.io.FileUtils;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class HelixInstanceDataManagerTest {

  private HelixInstanceDataManager _helixInstanceDataManager;
  private File _testInstanceDataDir;
  private File _testInstanceSegmentTarDir;

  @BeforeClass
  public void setUp() {
    _helixInstanceDataManager = Mockito.mock(HelixInstanceDataManager.class);
    _testInstanceDataDir = new File("testInstanceDataDir");
    _testInstanceSegmentTarDir = new File("testInstanceSegmentTarDir");
    Mockito.doCallRealMethod().when(_helixInstanceDataManager).initInstanceDataDir(_testInstanceDataDir);
    Mockito.doCallRealMethod().when(_helixInstanceDataManager).initInstanceSegmentTarDir(_testInstanceSegmentTarDir);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(_testInstanceDataDir);
    FileUtils.deleteQuietly(_testInstanceSegmentTarDir);
  }

  @Test
  public void testInitInstanceDataDirCreatesDirectory() {
    if (_testInstanceDataDir.exists()) {
      FileUtils.deleteQuietly(_testInstanceDataDir);
    }

    _helixInstanceDataManager.initInstanceDataDir(_testInstanceDataDir);

    assertTrue(_testInstanceDataDir.exists(), "Instance data directory should be created.");
  }

  @Test
  public void testInitInstanceDataDirCleansUpTempDirs() {
    File tableDir = new File(_testInstanceDataDir, "tableA_OFFLINE");
    assertTrue(tableDir.mkdirs(), "Failed to create temp directory.");

    _helixInstanceDataManager.initInstanceDataDir(_testInstanceDataDir);

    assertFalse(tableDir.exists(), "Temporary directory should be deleted.");
  }

  @Test
  public void testInitInstanceDataDirThrowsIfNotWritable() {
    if (!_testInstanceDataDir.exists()) {
      assertTrue(_testInstanceDataDir.mkdirs(), "Failed to create test directory.");
    }
    _testInstanceDataDir.setWritable(false);

    try {
      _helixInstanceDataManager.initInstanceDataDir(_testInstanceDataDir);
      fail("Expected IllegalStateException due to non-writable directory.");
    } catch (IllegalStateException e) {
      // Expected exception
    } finally {
      _testInstanceDataDir.setWritable(true);
    }
  }

  @Test
  public void testInitInstanceSegmentTarDirCreatesDirectory() {
    if (_testInstanceSegmentTarDir.exists()) {
      FileUtils.deleteQuietly(_testInstanceSegmentTarDir);
    }

    _helixInstanceDataManager.initInstanceSegmentTarDir(_testInstanceSegmentTarDir);

    assertTrue(_testInstanceSegmentTarDir.exists(), "Instance segment tar directory should be created.");
  }

  @Test
  public void testInitInstanceSegmentTarDirThrowsIfNotWritable() {
    if (!_testInstanceSegmentTarDir.exists()) {
      assertTrue(_testInstanceSegmentTarDir.mkdirs(), "Failed to create test directory.");
    }
    _testInstanceSegmentTarDir.setWritable(false);

    try {
      _helixInstanceDataManager.initInstanceSegmentTarDir(_testInstanceSegmentTarDir);
      fail("Expected IllegalStateException due to non-writable directory.");
    } catch (IllegalStateException e) {
      // Expected exception
    } finally {
      _testInstanceSegmentTarDir.setWritable(true);
    }
  }
}
