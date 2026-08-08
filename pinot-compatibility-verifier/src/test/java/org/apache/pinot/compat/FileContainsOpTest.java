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
package org.apache.pinot.compat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests exact root-cause matching across compatibility-runner log files.
public class FileContainsOpTest {

  @Test
  public void testContainsExpectedTextRequiresMatchingFileAndText()
      throws Exception {
    Path directory = Files.createTempDirectory("pinot-file-contains-op");
    Path serverLog = directory.resolve("server.4.log");
    Path brokerLog = directory.resolve("broker.3.log");
    try {
      Files.writeString(serverLog,
          "Caused by: java.lang.IllegalStateException: Unsupported proto ColumnDataType: UNRECOGNIZED\n",
          StandardCharsets.UTF_8);
      Files.writeString(brokerLog, "Caught exception while deserializing stage plan\n", StandardCharsets.UTF_8);

      assertTrue(FileContainsOp.containsExpectedText(directory, "server*.log",
          "Unsupported proto ColumnDataType: UNRECOGNIZED"));
      assertFalse(FileContainsOp.containsExpectedText(directory, "server*.log",
          "Caught exception while deserializing stage plan"));
      assertFalse(FileContainsOp.containsExpectedText(directory, "missing.*.log",
          "Unsupported proto ColumnDataType: UNRECOGNIZED"));
    } finally {
      Files.deleteIfExists(serverLog);
      Files.deleteIfExists(brokerLog);
      Files.deleteIfExists(directory);
    }
  }
}
