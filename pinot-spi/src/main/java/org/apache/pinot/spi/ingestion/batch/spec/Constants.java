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
package org.apache.pinot.spi.ingestion.batch.spec;

import java.util.Map;

public class Constants {
  private Constants() {
  }

  /// By default Pinot segments are compressed in 'tar.gz' format then pushed to controller.
  public static final String TAR_GZ_FILE_EXT = ".tar.gz";
  public static final String METADATA_TAR_GZ_FILE_EXT = ".metadata.tar.gz";

  /// Archive extensions understood by segment upload and metadata extraction, including existing LZ4 archives.
  public static final Map<String, String> SEGMENT_TAR_FILE_EXTENSIONS =
      Map.of("gzip", TAR_GZ_FILE_EXT, "lz4", ".tar.lz4", "zstd", ".tar.zst");

  /// Returns whether a path names a supported segment archive, excluding the gzip metadata sidecars.
  public static boolean isSegmentTarFile(String fileName) {
    return !fileName.endsWith(METADATA_TAR_GZ_FILE_EXT)
        && SEGMENT_TAR_FILE_EXTENSIONS.values().stream().anyMatch(fileName::endsWith);
  }
}
