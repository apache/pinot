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
package org.apache.pinot.segment.spi.loader;

import java.net.URI;
import org.apache.pinot.segment.spi.store.SegmentDirectory;


/**
 * Interface for creating and loading the {@link SegmentDirectory} instance using provided config
 */
public interface SegmentDirectoryLoader {

  /**
   * Creates the {@link SegmentDirectory} instance
   * @param indexDir index directory
   * @param segmentDirectoryLoaderContext context for loading this segment
   */
  SegmentDirectory load(URI indexDir, SegmentDirectoryLoaderContext segmentDirectoryLoaderContext)
      throws Exception;

  /**
   * Clean up the segment data from the server.
   * @param segmentDirectoryLoaderContext context for cleaning up segment data
   */
  default void delete(SegmentDirectoryLoaderContext segmentDirectoryLoaderContext)
      throws Exception {
  }

  /**
   * Based on the zkMetadata's and current segment tier, checks whether or not tier migration is needed
   * @param targetTier segment's ZKMetadata's tier
   * @param currentTier Current segment tier
   */
  default boolean needsTierMigration(String targetTier, String currentTier) {
    return false;
  }
}
