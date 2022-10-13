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
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentLoader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
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
   * {@link SegmentDirectory}
   * @param indexDir segment index directory
   * @param segmentLoaderContext context for instantiation of the SegmentDirectory
   * @return instance of {@link SegmentLocalFSDirectory}
   */
  @Override
  public SegmentDirectory load(URI indexDir, SegmentDirectoryLoaderContext segmentLoaderContext)
      throws Exception {
    PinotConfiguration segmentDirectoryConfigs = segmentLoaderContext.getSegmentDirectoryConfigs();
    File directory = new File(indexDir);
    if (!directory.exists()) {
      return new SegmentLocalFSDirectory(directory);
    }
    return new SegmentLocalFSDirectory(directory,
        ReadMode.valueOf(segmentDirectoryConfigs.getProperty(IndexLoadingConfig.READ_MODE_KEY)));
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
