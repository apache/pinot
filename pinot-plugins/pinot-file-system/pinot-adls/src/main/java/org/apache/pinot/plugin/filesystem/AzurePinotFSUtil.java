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
package org.apache.pinot.plugin.filesystem;

import java.io.File;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;


/**
 * Util class for Azure related PinotFS
 */
public class AzurePinotFSUtil {
  private static final String DIRECTORY_DELIMITER = File.separator;

  private AzurePinotFSUtil() {
  }

  /**
   * Extract Azure Data Lake Gen2 style path from uri
   *
   * NOTE: returning path 'should not be' url encoded. (e.g. should return 'a/segment' instead of 'a%2Fsegment')
   *
   * @param uri a uri path
   * @return path in Azure Data Lake Gen2 format
   */
  public static String convertUriToAzureStylePath(URI uri) {
    // Pinot side code uses `URLEncoder` when building uri
    String path = URLDecoder.decode(uri.getRawPath(), StandardCharsets.UTF_8);
    if (path.startsWith(DIRECTORY_DELIMITER)) {
      path = path.substring(1);
    }
    if (path.endsWith(DIRECTORY_DELIMITER)) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

  /**
   * Convert Azure Data Lake Gen2 style path into uri format.
   *
   * NOTE: returning path 'should not be' url encoded. (e.g. should return 'a/segment' instead of 'a%2Fsegment')
   *
   * @param path Azure data lake gen2 style path
   * @return path in uri format
   */
  public static String convertAzureStylePathToUriStylePath(String path) {
    if (!path.startsWith(DIRECTORY_DELIMITER)) {
      path = DIRECTORY_DELIMITER + path;
    }
    if (path.endsWith(DIRECTORY_DELIMITER)) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }
}
