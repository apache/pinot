/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class ZookeeperLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperLauncher.class);

  private ZkServer zkServer;
  String dataDir = "/tmp/zk/data";
  String logDir = "/tmp/zk/logs";
  private IDefaultNameSpace defaultNameSpace;
  private String zkHost;
  private int zkPort;

  public ZookeeperLauncher() {
  }

  public static File createAutoDeleteTempDir() {
    File tempdir = Files.createTempDir();
    tempdir.delete();
    tempdir.mkdir();
    tempdir.deleteOnExit();
    return tempdir;
  }

  public boolean start(int port) {
    
    File tmpdir = createAutoDeleteTempDir();
    File logdir = new File(tmpdir + File.separator + "translog");
    File datadir = new File(tmpdir + File.separator + "snapshot");

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient) {
        // init any zk paths if needed
      }
    };
    ZkServer zkServer =
        new ZkServer(datadir.getAbsolutePath(), logdir.getAbsolutePath(), defaultNameSpace, port, 30000, 60000);
    zkServer.start();

    LOGGER.info("Start zookeeper at localhost:" + zkServer.getPort() + " in thread "
        + Thread.currentThread().getName());
    return true;
  }

  public boolean stop() {
    zkServer.shutdown();
    return true;
  }

  public static void main(String[] args) throws Exception {

    ZookeeperLauncher launcher = new ZookeeperLauncher();
    launcher.start(2188);

  }

}
