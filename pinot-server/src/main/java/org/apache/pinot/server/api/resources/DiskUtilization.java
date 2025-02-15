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
package org.apache.pinot.server.api.resources;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.restlet.resources.DiskUsageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DiskUtilization {
  private static final Logger LOGGER = LoggerFactory.getLogger(DiskUtilization.class);

  private DiskUtilization() {
  }
  /**
   * Computes the disk usage for the file store containing the given path using NIO.
   */
  public static DiskUsageInfo computeDiskUsage(String instanceId, String pathStr) throws IOException {
    if (StringUtils.isEmpty(instanceId)) {
      throw new IllegalArgumentException("InstanceID cannot be null or empty while computing disk utilization.");
    }

    if (StringUtils.isEmpty(pathStr)) {
      throw new IllegalArgumentException("Path cannot be null or empty while computing disk utilization.");
    }

    Path path = Paths.get(pathStr);
    if (!Files.exists(path)) {
      throw new IllegalArgumentException("The path " + pathStr + " does not exist.");
    }

    // Obtain the file store corresponding to the path.
    FileStore store = Files.getFileStore(path);

    long totalSpace = store.getTotalSpace();
    // getUnallocatedSpace() returns the number of bytes that are unallocated in the file store,
    // which corresponds to the "free space" on disk.
    long freeSpace = store.getUnallocatedSpace();
    long usedSpace = totalSpace - freeSpace;
    LOGGER.info("Disk usage for instance: {} at path: {} totalSpace: {}, usedSpace: {}", instanceId, pathStr,
        totalSpace, usedSpace);
    return new DiskUsageInfo(instanceId, path.toString(), totalSpace, usedSpace, System.currentTimeMillis());
  }
}
