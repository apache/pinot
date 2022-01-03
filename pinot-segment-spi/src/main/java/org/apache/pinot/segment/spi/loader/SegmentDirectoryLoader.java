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

import java.io.File;
import java.net.URI;
import org.apache.pinot.segment.spi.store.SegmentDirectory;


/**
 * Interface for creating and loading the {@link SegmentDirectory} instance using provided config.
 *
 * The segment may be kept in local or remote tier backend. When the segment needs reprocessing,
 * like to add or remove indices, the SegmentDirectoryLoader downloads the segment from tier backend
 * to a local directory to reprocess it, then uploads the reprocessed segment back to the tier backend.
 * If the tier backend is local disk (which is by default), download and upload operations can be noop.
 * If the tier backend is some remote store, the two operations would transfer data over the network.
 *
 * The download and upload methods are necessary for now as the segment reprocessing logic as implemented
 * in the SegmentPreProcessor class only works with local segment directory.
 */
public interface SegmentDirectoryLoader {

  /**
   * Upload the processed segment from a local directory to tier backend.
   * @param indexDir the local directory of processed segment to upload.
   * @param segmentDirectoryLoaderContext context for uploading this segment, like where to keep
   *                                      the segment in the tier backend.
   */
  void upload(File indexDir, SegmentDirectoryLoaderContext segmentDirectoryLoaderContext)
      throws Exception;

  /**
   * Download the processed segment from the tier backend to a local directory.
   * @param indexDir the destination directory to download the processed segment to.
   * @param segmentDirectoryLoaderContext context for downloading this segment, like where to find
   *                                      the segment that's kept in the tier backend.
   */
  void download(File indexDir, SegmentDirectoryLoaderContext segmentDirectoryLoaderContext)
      throws Exception;

  /**
   * Creates the {@link SegmentDirectory} instance with the provided context.
   * @param indexDir index directory from local or remote tier backend. This may be overridden by
   *                 the index directory derived from the context, depending on the implementation.
   * @param segmentDirectoryLoaderContext context for loading this segment.
   */
  SegmentDirectory load(URI indexDir, SegmentDirectoryLoaderContext segmentDirectoryLoaderContext)
      throws Exception;
}
