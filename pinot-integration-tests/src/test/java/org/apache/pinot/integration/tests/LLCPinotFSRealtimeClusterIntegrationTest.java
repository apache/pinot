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
package org.apache.pinot.integration.tests;

import java.io.File;
import java.nio.file.Files;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.ControllerConf;
import org.testng.annotations.BeforeClass;


public class LLCPinotFSRealtimeClusterIntegrationTest extends LLCRealtimeClusterIntegrationTest {
  public static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  public static final long RANDOM_SEED = System.currentTimeMillis();
  private static MiniDFSCluster hdfsCluster;

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    // Build a local HDFS cluster.
    File baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    hdfsCluster = builder.build();
    // Remove the consumer directory
    File consumerDirectory = new File(CONSUMER_DIRECTORY);
    if (consumerDirectory.exists()) {
      FileUtils.deleteDirectory(consumerDirectory);
    }

    super.setUp();
  }

  @Override
  public void startController() {
    this.startController(getControllerWithDeepStorageConfiguration());
  }

  public ControllerConf getControllerWithDeepStorageConfiguration() {
    ControllerConf config = new ControllerConf();
    config.setControllerHost(LOCAL_HOST);
    config.setControllerPort(Integer.toString(DEFAULT_CONTROLLER_PORT));
    // Config to use remote HDFS storage and local tmp directory.
    config.setDataDir("hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/");
    config.setLocalTempDir("/tmp");
    // Config the Pinot FS class.
    Configuration pinotFSConfig = new BaseConfiguration();
    pinotFSConfig.setProperty("pinot.controller.storage.factory.class.hdfs",
        "org.apache.pinot.integration.tests.MockHadoopPinotFS");
    config.setPinotFSFactoryClasses(pinotFSConfig);
    // Config the HDFS client default FS.
    config.setProperty("pinot.controller.storage.factory.hdfs.fs.defaultFS",
        "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/");
    config.setProperty("pinot.controller.storage.factory.hdfs.secure", "false");
    config.setZkStr(ZkStarter.DEFAULT_ZK_STR);
    config.setHelixClusterName(getClass().getSimpleName());
    return config;
  }
}
