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
package org.apache.pinot.spark.utils;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotSparkJobPreparationHelper {
  private static final Logger _logger = LoggerFactory.getLogger(PinotSparkJobPreparationHelper.class);

  public static void addDepsJarToDistributedCacheHelper(FileSystem fileSystem, JavaSparkContext sparkContext,
      Path depsJarDir)
      throws IOException {
    FileStatus[] fileStatuses = fileSystem.listStatus(depsJarDir);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        addDepsJarToDistributedCacheHelper(fileSystem, sparkContext, fileStatus.getPath());
      } else {
        Path depJarPath = fileStatus.getPath();
        if (depJarPath.getName().endsWith(".jar")) {
          _logger.info("Adding deps jar: {} to distributed cache", depJarPath);
          sparkContext.addJar(depJarPath.toUri().getPath());
        }
      }
    }
  }
}
