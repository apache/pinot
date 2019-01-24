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
package org.apache.pinot.tools.perf;

import com.google.common.base.Preconditions;
import java.io.File;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZookeeperLauncher {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperLauncher.class);

  private final File _tempDir;
  private final String _dataDir;
  private final String _logDir;

  private ZkServer _zkServer;

  public ZookeeperLauncher() {
    this("/tmp");
  }

  public ZookeeperLauncher(String baseTempDir) {
    _tempDir = new File(baseTempDir, "zk_" + String.valueOf(System.currentTimeMillis()));
    if (_tempDir.exists()) {
      Preconditions.checkArgument(_tempDir.delete());
    }
    Preconditions.checkArgument(_tempDir.mkdirs());

    _dataDir = new File(_tempDir, "data").getAbsolutePath();
    _logDir = new File(_tempDir, "logs").getAbsolutePath();
  }

  public boolean start(int zkPort) {
    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient) {
        // init any zk paths if needed
      }
    };
    LOGGER.info("Starting zookeeper at localhost:{} in thread: {}", zkPort, Thread.currentThread().getName());
    _zkServer = new ZkServer(_dataDir, _logDir, defaultNameSpace, zkPort, 30000, 60000);
    _zkServer.start();
    return true;
  }

  public boolean stop() {
    _zkServer.shutdown();
    return _tempDir.delete();
  }

  public static void main(String[] args)
      throws Exception {
    ZookeeperLauncher launcher = new ZookeeperLauncher();
    launcher.start(2188);
  }
}
