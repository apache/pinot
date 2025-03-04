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
package org.apache.pinot.controller.helix.core;

import org.apache.helix.model.StateModelDefinition;


/**
 * Segment state model generator describes the transitions for segment states.
 *
 * Online to Offline, Online to Dropped
 * Consuming to Online, Consuming to Offline, Consuming to Dropped
 * Offline to Online, Offline to Consuming, Offline to Dropped
 *
 */
public class PinotHelixSegmentOnlineOfflineStateModelGenerator {
  private PinotHelixSegmentOnlineOfflineStateModelGenerator() {
  }

  public static final String PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL = "SegmentOnlineOfflineStateModel";

  public static final String ONLINE_STATE = "ONLINE";
  public static final String OFFLINE_STATE = "OFFLINE";
  public static final String DROPPED_STATE = "DROPPED";
  public static final String CONSUMING_STATE = "CONSUMING";

  // Helix state transitions can be assigned a priority which is used by Helix
  // to issue the order of state transitions. A lower priority value means a higher
  // priority in Helix.
  // Set all DROPPED related state transitions to have a higher priority.
  // Handling DROPPED state transitions with a higher priority can help prevent
  // servers from running into disk utilization problems.
  public static final int DROPPED_STATE_TRANSITION_PRIORITY = 1;
  public static final int DEFAULT_STATE_TRANSITION_PRIORITY = Integer.MAX_VALUE;

  public static StateModelDefinition generatePinotStateModelDefinition() {
    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL);
    // Set the initial state when the node starts
    builder.initialState(OFFLINE_STATE);

    builder.addState(ONLINE_STATE);
    builder.addState(CONSUMING_STATE);
    builder.addState(OFFLINE_STATE);
    builder.addState(DROPPED_STATE);

    // Add transitions between the states.
    builder.addTransition(CONSUMING_STATE, ONLINE_STATE, DEFAULT_STATE_TRANSITION_PRIORITY);
    builder.addTransition(OFFLINE_STATE, CONSUMING_STATE, DEFAULT_STATE_TRANSITION_PRIORITY);
    builder.addTransition(OFFLINE_STATE, ONLINE_STATE, DEFAULT_STATE_TRANSITION_PRIORITY);
    builder.addTransition(CONSUMING_STATE, OFFLINE_STATE, DEFAULT_STATE_TRANSITION_PRIORITY);
    builder.addTransition(ONLINE_STATE, OFFLINE_STATE, DEFAULT_STATE_TRANSITION_PRIORITY);
    // Add explicit state transitions to DROPPED from each state to ensure that DROPPED can be processed in a single
    // state transition. Without adding explicit state transitions to DROPPED from ONLINE/CONSUMING, two state
    // transitions will be needed to fully drop a segment (CONSUMING/ONLINE -> OFFLINE, OFFLINE -> DROPPED)
    builder.addTransition(OFFLINE_STATE, DROPPED_STATE, DROPPED_STATE_TRANSITION_PRIORITY);
    builder.addTransition(ONLINE_STATE, DROPPED_STATE, DROPPED_STATE_TRANSITION_PRIORITY);
    builder.addTransition(CONSUMING_STATE, DROPPED_STATE, DROPPED_STATE_TRANSITION_PRIORITY);

    // set constraints on states.
    // static constraint
    builder.dynamicUpperBound(ONLINE_STATE, "R");
    // dynamic constraint, R means it should be derived based on the replication
    // factor.
    builder.dynamicUpperBound(CONSUMING_STATE, "R");

    StateModelDefinition statemodelDefinition = builder.build();
    return statemodelDefinition;
  }
}
