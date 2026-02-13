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
package org.apache.pinot.integration.tests.multicluster;

/**
 * Integration tests for multi-cluster routing when the SAME physical table name exists in both clusters.
 * This class extends {@link MultiClusterIntegrationTest} and inherits all its tests, but configures
 * both clusters to use identical physical table names (unlike the parent which uses different names).
 */
public class SameTableNameMultiClusterIntegrationTest extends MultiClusterIntegrationTest {
  /**
   * Override to use cluster1's table name in cluster2 as well (same physical table name).
   */
  @Override
  protected String getPhysicalTable1InCluster2() {
    return getPhysicalTable1InCluster1();
  }

  /**
   * Override to use cluster1's table name in cluster2 as well (same physical table name).
   */
  @Override
  protected String getPhysicalTable2InCluster2() {
    return getPhysicalTable2InCluster1();
  }
}
