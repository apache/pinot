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
 * Consuming to Online, Consuming to Offline
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

  // In the state model, the priority of the transition is important.
  // The transition with higher priority will be executed first.
  // The priority is an integer, LOWER value means HIGHER priority.
  // Making transitioning to OFFLINE and DROP higher priority.
  // This can prevent server from running into resource/storage problems
  public static final int DEFAULT_TO_DROP_TRANSITION_PRIORITY = 800;
  public static final int DEFAULT_TO_OFFLINE_TRANSITION_PRIORITY = 900;
  public static final int DEFAULT_TO_ONLINE_TRANSITION_PRIORITY = 1000;
  // Given a lower priority to let the consuming segment start last during server startup.
  public static final int OFFLINE_TO_CONSUMING_TRANSITION_PRIORITY = 1100;

  public static StateModelDefinition generatePinotStateModelDefinition() {
    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL);
    builder.initialState(OFFLINE_STATE);

    builder.addState(ONLINE_STATE);
    builder.addState(CONSUMING_STATE);
    builder.addState(OFFLINE_STATE);
    builder.addState(DROPPED_STATE);
    // Set the initial state when the node starts

    // Add transitions between the states.
    // Ordering the transitions by priority, high -> low.
    builder.addTransition(OFFLINE_STATE, DROPPED_STATE, DEFAULT_TO_DROP_TRANSITION_PRIORITY);
    builder.addTransition(CONSUMING_STATE, OFFLINE_STATE, DEFAULT_TO_OFFLINE_TRANSITION_PRIORITY);
    builder.addTransition(ONLINE_STATE, OFFLINE_STATE, DEFAULT_TO_OFFLINE_TRANSITION_PRIORITY);
    builder.addTransition(CONSUMING_STATE, ONLINE_STATE, DEFAULT_TO_ONLINE_TRANSITION_PRIORITY);
    builder.addTransition(OFFLINE_STATE, ONLINE_STATE, DEFAULT_TO_ONLINE_TRANSITION_PRIORITY);
    builder.addTransition(OFFLINE_STATE, CONSUMING_STATE, OFFLINE_TO_CONSUMING_TRANSITION_PRIORITY);

    // set constraints on states.
    // static constraint
    builder.dynamicUpperBound(ONLINE_STATE, "R");
    // dynamic constraint, R means it should be derived based on the replication
    // factor.
    builder.dynamicUpperBound(CONSUMING_STATE, "R");

    return builder.build();
  }
}
