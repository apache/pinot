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
package org.apache.pinot.plugin.minion.tasks;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class MinionTaskUtilsTest {

  @Test
  public void testGetInputPinotFS() throws Exception {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put("input.fs.className", "org.apache.pinot.spi.filesystem.LocalPinotFS");
    URI fileURI = new URI("file:///path/to/file");

    PinotFS pinotFS = MinionTaskUtils.getInputPinotFS(taskConfigs, fileURI);

    assertTrue(pinotFS instanceof LocalPinotFS);
  }

  @Test
  public void testGetOutputPinotFS() throws Exception {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put("output.fs.className", "org.apache.pinot.spi.filesystem.LocalPinotFS");
    URI fileURI = new URI("file:///path/to/file");

    PinotFS pinotFS = MinionTaskUtils.getOutputPinotFS(taskConfigs, fileURI);

    assertTrue(pinotFS instanceof LocalPinotFS);
  }

  @Test
  public void testIsLocalOutputDir() {
    assertTrue(MinionTaskUtils.isLocalOutputDir("file"));
    assertFalse(MinionTaskUtils.isLocalOutputDir("hdfs"));
  }

  @Test
  public void testGetLocalPinotFs() {
    assertTrue(MinionTaskUtils.getLocalPinotFs() instanceof LocalPinotFS);
  }

  @Test
  public void testNormalizeDirectoryURI() {
    assertEquals("file:///path/to/dir/", MinionTaskUtils.normalizeDirectoryURI("file:///path/to/dir"));
    assertEquals("file:///path/to/dir/", MinionTaskUtils.normalizeDirectoryURI("file:///path/to/dir/"));
  }

  @Test
  public void testGetSegmentServerUrisList() throws Exception {
    // empty list test
    Map<String, String> configs = new HashMap<>();
    Map<String, List<String>> result = MinionTaskUtils.getSegmentServerUrisList(configs);
    assertTrue(result.isEmpty());

    configs = new HashMap<>();
    configs.put(MinionConstants.SEGMENT_SERVER_URIS_LIST_KEY, "{\"segment1\": [\"http://localhost:8080\", "
        + "\"http://localhost:8081\"], \"segment2\": [\"http://localhost:8082\"]}");
    result = MinionTaskUtils.getSegmentServerUrisList(configs);
    assertEquals(2, result.size());
    assertTrue(result.containsKey("segment1"));
    assertTrue(result.containsKey("segment2"));
    assertEquals(List.of("http://localhost:8080", "http://localhost:8081"), result.get("segment1"));
    assertEquals(List.of("http://localhost:8082"), result.get("segment2"));

    // wrong json
    try {
      configs = new HashMap<>();
      configs.put(MinionConstants.SEGMENT_SERVER_URIS_LIST_KEY, "{\"segment1\": [\"http://localhost:8080\", "
          + "\"http://localhost:8081\"], \"segment2\": [\"http://localhost:8082\"]");
      MinionTaskUtils.getSegmentServerUrisList(configs);
      Assert.fail("Should have failed due to invalid json");
    } catch (Exception e) {
      // expected
    }
  }
}
