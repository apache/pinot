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
package org.apache.pinot.common.utils.log;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Set;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class LocalLogFileServerTest {

  @Test
  public void testLoggerFileServer()
      throws IOException {
    File logRootDir = new File(FileUtils.getTempDirectory(), "testGetAllLoggers-" + System.currentTimeMillis());
    try {
      logRootDir.mkdirs();
      LogFileServer logFileServer = new LocalLogFileServer(logRootDir.getAbsolutePath());

      // Empty root log directory
      assertEquals(logFileServer.getAllLogFilePaths().size(), 0);
      try {
        logFileServer.downloadLogFile("log1");
        Assert.fail("Shouldn't reach here");
      } catch (WebApplicationException e1) {
        assertEquals(e1.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
      }

      // 1 file: [ log1 ] in root log directory
      FileUtils.writeStringToFile(new File(logRootDir, "log1"), "mylog1", Charset.defaultCharset());
      assertEquals(logFileServer.getAllLogFilePaths().size(), 1);
      assertNotNull(logFileServer.downloadLogFile("log1"));
      try {
        logFileServer.downloadLogFile("log2");
        Assert.fail("Shouldn't reach here");
      } catch (WebApplicationException e1) {
        assertEquals(e1.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
      }

      // 2 files: [ log1, log2 ] in root log directory
      FileUtils.writeStringToFile(new File(logRootDir, "log2"), "mylog2", Charset.defaultCharset());
      assertEquals(logFileServer.getAllLogFilePaths().size(), 2);
      assertNotNull(logFileServer.downloadLogFile("log1"));
      assertNotNull(logFileServer.downloadLogFile("log2"));
      try {
        logFileServer.downloadLogFile("log3");
        Assert.fail("Shouldn't reach here");
      } catch (WebApplicationException e1) {
        assertEquals(e1.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
      }
    } finally {
      FileUtils.deleteQuietly(logRootDir);
    }
  }

  /// Verifies that {@link LocalLogFileServer#getAllLogFilePaths()} enumerates files inside nested
  /// subdirectories and returns paths that are relative to the log root. This is a regression test
  /// for the {@code Files.walk} refactor that wraps the stream in a try-with-resources block; the
  /// recursion behavior must be preserved so that downloads under nested directories continue to
  /// work.
  @Test
  public void testGetAllLogFilePathsEnumeratesNestedDirectories()
      throws IOException {
    File logRootDir = new File(FileUtils.getTempDirectory(),
        "testGetAllLogFilePathsEnumeratesNestedDirectories-" + System.currentTimeMillis());
    try {
      assertTrue(logRootDir.mkdirs());
      File nested = new File(logRootDir, "sub/dir");
      assertTrue(nested.mkdirs());
      FileUtils.writeStringToFile(new File(logRootDir, "top.log"), "top", Charset.defaultCharset());
      FileUtils.writeStringToFile(new File(nested, "nested.log"), "nested", Charset.defaultCharset());

      LogFileServer logFileServer = new LocalLogFileServer(logRootDir.getAbsolutePath());
      Set<String> paths = logFileServer.getAllLogFilePaths();

      assertEquals(paths.size(), 2, "expected two enumerated files, got: " + paths);
      assertTrue(paths.contains("top.log"), "missing top.log in " + paths);
      assertTrue(paths.contains("sub/dir/nested.log"), "missing sub/dir/nested.log in " + paths);
      // Both files must be downloadable via the relative paths returned above.
      assertNotNull(logFileServer.downloadLogFile("top.log"));
      assertNotNull(logFileServer.downloadLogFile("sub/dir/nested.log"));
    } finally {
      FileUtils.deleteQuietly(logRootDir);
    }
  }
}
