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
 * Constants for rebalance config properties
 */
public class RebalanceConfigConstants {

  // Whether to rebalance table in dry-run mode
  public static final String DRY_RUN = "dryRun";
  public static final boolean DEFAULT_DRY_RUN = false;

  // Whether to reassign instances before reassigning segments
  public static final String REASSIGN_INSTANCES = "reassignInstances";
  public static final boolean DEFAULT_REASSIGN_INSTANCES = false;

  // Whether to reassign CONSUMING segments
  public static final String INCLUDE_CONSUMING = "includeConsuming";
  public static final boolean DEFAULT_INCLUDE_CONSUMING = false;

  // Whether to rebalance table in bootstrap mode (regardless of minimum segment movement, reassign all segments in a
  // round-robin fashion as if adding new segments to an empty table)
  public static final String BOOTSTRAP = "bootstrap";
  public static final boolean DEFAULT_BOOTSTRAP = false;

  // Whether to allow downtime for the rebalance
  public static final String DOWNTIME = "downtime";
  public static final boolean DEFAULT_DOWNTIME = false;

  // For no-downtime rebalance, minimum number of replicas to keep alive during rebalance, or maximum number of replicas
  // allowed to be unavailable if value is negative
  public static final String MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME = "minReplicasToKeepUpForNoDowntime";
  public static final int DEFAULT_MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME = 1;

  // Whether to use best-efforts to rebalance (not fail the rebalance when the no-downtime contract cannot be achieved)
  // When using best-efforts to rebalance, the following scenarios won't fail the rebalance (will log warnings instead):
  // - Segment falls into ERROR state in ExternalView -> count ERROR state as good state
  // - ExternalView has not converged within the maximum wait time -> continue to the next stage
  public static final String BEST_EFFORTS = "bestEfforts";
  public static final boolean DEFAULT_BEST_EFFORTS = false;
}
