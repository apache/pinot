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
package org.apache.pinot.plugin.stream.kafka.utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;


public class EmbeddedZooKeeper implements Closeable {
  private static final int TICK_TIME = 500;

  private final NIOServerCnxnFactory _factory;
  private final String _zkAddress;

  public EmbeddedZooKeeper(File workingDir)
      throws IOException, InterruptedException {
    _factory = new NIOServerCnxnFactory();
    ZooKeeperServer zkServer =
        new ZooKeeperServer(new File(workingDir, "data"), new File(workingDir, "log"), TICK_TIME);
    _factory.configure(new InetSocketAddress("localhost", 0), 0);
    _factory.startup(zkServer);
    _zkAddress = "localhost:" + zkServer.getClientPort();
  }

  public String getZkAddress() {
    return _zkAddress;
  }

  @Override
  public void close()
      throws IOException {
    _factory.shutdown();
  }
}
