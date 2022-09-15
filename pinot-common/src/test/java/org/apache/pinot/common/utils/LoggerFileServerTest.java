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
package org.apache.pinot.common.utils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class LoggerFileServerTest {

  @Test
  public void testLoggerFileServer()
      throws IOException {
    File logRootDir = new File(FileUtils.getTempDirectory(), "testGetAllLoggers-" + System.currentTimeMillis());
    try {
      logRootDir.mkdirs();
      LoggerFileServer loggerFileServer = new LoggerFileServer(logRootDir.getAbsolutePath());

      // Empty root log directory
      assertEquals(loggerFileServer.getAllPaths().size(), 0);
      try {
        loggerFileServer.downloadLogFile("log1");
        Assert.fail("Shouldn't reach here");
      } catch (WebApplicationException e1) {
        assertEquals(e1.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
      }

      // 1 file: [ log1 ] in root log directory
      FileUtils.writeStringToFile(new File(logRootDir, "log1"), "mylog1", Charset.defaultCharset());
      assertEquals(loggerFileServer.getAllPaths().size(), 1);
      assertNotNull(loggerFileServer.downloadLogFile("log1"));
      try {
        loggerFileServer.downloadLogFile("log2");
        Assert.fail("Shouldn't reach here");
      } catch (WebApplicationException e1) {
        assertEquals(e1.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
      }

      // 2 files: [ log1, log2 ] in root log directory
      FileUtils.writeStringToFile(new File(logRootDir, "log2"), "mylog2", Charset.defaultCharset());
      assertEquals(loggerFileServer.getAllPaths().size(), 2);
      assertNotNull(loggerFileServer.downloadLogFile("log1"));
      assertNotNull(loggerFileServer.downloadLogFile("log2"));
      try {
        loggerFileServer.downloadLogFile("log3");
        Assert.fail("Shouldn't reach here");
      } catch (WebApplicationException e1) {
        assertEquals(e1.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
      }
    } finally {
      FileUtils.deleteQuietly(logRootDir);
    }
  }
}
