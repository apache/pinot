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
package org.apache.pinot.controller.helix.core.rebalance;

/**
 * Constants for rebalance user config properties
 */
public class RebalanceConfigConstants {
  // Whether to run rebalancer in dry-run mode
  public static final String DRY_RUN = "dryRun";
  public static final boolean DEFAULT_DRY_RUN = false;

  // Whether to reassign instances
  public static final String REASSIGN_INSTANCES = "reassignInstances";
  public static final boolean DEFAULT_REASSIGN_INSTANCES = false;

  // Whether to rebalance CONSUMING segments
  public static final String INCLUDE_CONSUMING = "includeConsuming";
  public static final boolean DEFAULT_INCLUDE_CONSUMING = false;

  // Whether to rebalance in with-downtime mode or no-downtime mode
  public static final String DOWNTIME = "downtime";
  public static final boolean DEFAULT_DOWNTIME = false;

  // Minimum replicas to keep up for no-downtime mode, use negative number to represent the maximum replicas allowed to
  // be unavailable
  public static final String MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME = "minReplicasToKeepUpForNoDowntime";
  public static final int DEFAULT_MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME = 1;
}
