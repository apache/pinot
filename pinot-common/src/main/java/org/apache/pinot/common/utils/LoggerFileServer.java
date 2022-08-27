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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeSet;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;


/**
 * Logger file server.
 */
public class LoggerFileServer {
  private final File _loggerRootDir;
  private final Path _loggerRootDirPath;

  public LoggerFileServer(String loggerRootDir) {
    Preconditions.checkNotNull(loggerRootDir, "Logger root directory is null");
    _loggerRootDir = new File(loggerRootDir);
    Preconditions.checkState(_loggerRootDir.exists(), "Logger directory doesn't exists");
    _loggerRootDirPath = Paths.get(_loggerRootDir.getAbsolutePath());
  }

  public Set<String> getAllPaths()
      throws IOException {
    Set<String> allFiles = new TreeSet<>();
    Files.walk(_loggerRootDirPath).filter(Files::isRegularFile).forEach(
        f -> allFiles.add(f.toAbsolutePath().toString().replace(_loggerRootDirPath.toAbsolutePath() + "/", "")));
    return allFiles;
  }

  public Response downloadLogFile(String filePath) {
    try {
      if (!getAllPaths().contains(filePath)) {
        throw new WebApplicationException("Invalid file path: " + filePath, Response.Status.FORBIDDEN);
      }
      File logFile = new File(_loggerRootDir, filePath);
      if (!logFile.exists()) {
        throw new WebApplicationException("File: " + filePath + " doesn't exists", Response.Status.NOT_FOUND);
      }
      Response.ResponseBuilder builder = Response.ok();
      builder.entity(logFile);
      builder.entity((StreamingOutput) output -> Files.copy(logFile.toPath(), output));
      builder.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + logFile.getName());
      builder.header(HttpHeaders.CONTENT_LENGTH, logFile.length());
      return builder.build();
    } catch (IOException e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
