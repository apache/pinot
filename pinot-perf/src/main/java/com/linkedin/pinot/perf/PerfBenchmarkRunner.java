/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.perf;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.config.Tenant.TenantBuilder;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


/**
 * Launcher class to set up Pinot for perf testing 
 *
 */
public class PerfBenchmarkRunner {
  /**
   * The segments are already extracted into a directory
   * @throws Exception 
   */
  public static void setupWithPreLoadedSegments(String directory, String offlineTableName) throws Exception {
    //create conf with default values
    PerfBenchmarkDriverConf conf = new PerfBenchmarkDriverConf();
    conf.setStartBroker(true);
    conf.setStartController(true);
    conf.setStartServer(true);
    conf.setStartZookeeper(true);
    conf.setUploadIndexes(false);
    conf.setRunQueries(false);
    conf.setServerInstanceSegmentTarDir(null);
    conf.setServerInstanceDataDir(directory);
    conf.setConfigureResources(false);
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    driver.run();

    Set<String> tables = new HashSet<String>();
    File[] segments = new File(directory, offlineTableName).listFiles();
    for (File segmentDir : segments) {
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDir);
      if (!tables.contains(segmentMetadata.getTableName())) {
        driver.configureTable(segmentMetadata.getTableName());
        tables.add(segmentMetadata.getTableName());
      }
      driver.addSegment(segmentMetadata);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length > 0) {
      if (args[0].equalsIgnoreCase("setupWithPreLoadedSegments")) {
        String offlineTableName = args[1];
        String indexRootDirectory = args[2];
        setupWithPreLoadedSegments(indexRootDirectory, offlineTableName);
      }
    } else {
      System.err.println("Expected one of [setupWithPreLoadedSegments]");
    }
  }
}
