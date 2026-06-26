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
package org.apache.pinot.segment.local.loader;

import java.io.File;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentLoader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Default implementation of {@link SegmentDirectoryLoader}
 */
@SegmentLoader(name = "default")
public class DefaultSegmentDirectoryLoader implements SegmentDirectoryLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSegmentDirectoryLoader.class);

  /**
   * Creates and loads the {@link SegmentLocalFSDirectory} which is the default implementation of
   * {@link SegmentDirectory}.
   *
   * <p>The {@link SegmentDirectoryLoaderContext} is forwarded into the {@link SegmentLocalFSDirectory}
   * so downstream consumers (e.g. {@code SingleFileIndexDirectory#createRemoteBuffers}) can use it to
   * propagate the table's task configuration into remote/empty index buffers.
   *
   * @param indexDir segment index directory
   * @param segmentLoaderContext context for instantiation of the SegmentDirectory
   * @return instance of {@link SegmentLocalFSDirectory}
   */
  @Override
  public SegmentDirectory load(URI indexDir, SegmentDirectoryLoaderContext segmentLoaderContext)
      throws Exception {
    File directory = new File(indexDir);
    if (!directory.exists()) {
      return new SegmentLocalFSDirectory(directory);
    }
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(directory);
    return new SegmentLocalFSDirectory(directory, metadata, segmentLoaderContext.getReadMode(),
        segmentLoaderContext);
  }

  @Override
  public void delete(SegmentDirectoryLoaderContext segmentLoaderContext)
      throws Exception {
    File indexDir = new File(segmentLoaderContext.getTableDataDir(), segmentLoaderContext.getSegmentName());
    if (indexDir.exists()) {
      FileUtils.deleteQuietly(indexDir);
      LOGGER.info("Deleted segment directory {} on default tier", indexDir);
    }
  }
}
