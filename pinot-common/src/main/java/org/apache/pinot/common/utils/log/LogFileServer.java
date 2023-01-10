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

import java.io.IOException;
import java.util.Set;
import javax.ws.rs.core.Response;

/**
 * Log file server interface
 */
public interface LogFileServer {
  /**
   * Returns all log file paths relative to logger root dir
   * @return a set of all log file paths relative to logger root dir
   * @throws IOException if there is problem reading all file paths
   */
  Set<String> getAllLogFilePaths() throws IOException;

  /**
   * Downloads a log file from the given file path (relative to logger root dir)
   * @param filePath file path relative to logger root dir
   * @return the log file content
   */
  Response downloadLogFile(String filePath);
}
