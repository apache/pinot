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

import org.testng.annotations.Test;


/**
 * Integration test that extends OfflineClusterIntegrationTest but start multiple brokers and servers.
 */
public class MultiNodesOfflineClusterIntegrationTest extends OfflineClusterIntegrationTest {
  private static final int NUM_BROKERS = 2;
  private static final int NUM_SERVERS = 3;

  @Override
  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  @Override
  protected int getNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected void startServers() {
    startServers(NUM_SERVERS);
  }

  // Disabled because gRPC query server is not enabled
  @Test(enabled = false)
  @Override
  public void testGrpcQueryServer() {
    // Ignored
  }

  @Test(enabled = false)
  @Override
  public void testHardcodedServerPartitionedSqlQueries() {
    // Ignored
  }
}
