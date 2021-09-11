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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.StateModelDefinition.StateModelDefinitionProperty;


/**
 * Broker resource state model generator describes the transitions for the resources
 * broker will serve.
 *
 * Online to Offline, Online to Dropped
 * Offline to Online, Offline to Dropped
 *
 *
 */
public class PinotHelixBrokerResourceOnlineOfflineStateModelGenerator {
  private PinotHelixBrokerResourceOnlineOfflineStateModelGenerator() {
  }

  public static final String PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL = "BrokerResourceOnlineOfflineStateModel";

  public static final String ONLINE_STATE = "ONLINE";
  public static final String OFFLINE_STATE = "OFFLINE";
  public static final String DROPPED_STATE = "DROPPED";

  public static StateModelDefinition generatePinotStateModelDefinition() {

    ZNRecord record = new ZNRecord(PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL);

    /*
     * initial state in always offline for an instance.
     *
     */
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(), OFFLINE_STATE);

    /*
     * this is a ondered list of states in which we want the instances to be in. the first entry is
     * given the top most priority.
     *
     */

    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add(ONLINE_STATE);
    statePriorityList.add(OFFLINE_STATE);
    statePriorityList.add(DROPPED_STATE);
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(), statePriorityList);

    /**
     *
     * If you are wondering what R and -1 signify, here is an explanation -1 means that don't even
     * try to keep any instances in this state. R says that all instances in the preference list
     * should be in this state.
     *
     */
    for (String state : statePriorityList) {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals(ONLINE_STATE)) {
        metadata.put("count", "R");
        record.setMapField(key, metadata);
      }
      if (state.equals(OFFLINE_STATE)) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      if (state.equals(DROPPED_STATE)) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
    }

    /*
     * construction a state transition table, this tells the controller the next state given initial
     * and final states.
     *
     */
    for (String state : statePriorityList) {
      String key = state + ".next";
      if (state.equals(ONLINE_STATE)) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put(OFFLINE_STATE, OFFLINE_STATE);
        metadata.put(DROPPED_STATE, DROPPED_STATE);
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put(ONLINE_STATE, ONLINE_STATE);
        metadata.put(DROPPED_STATE, DROPPED_STATE);
        record.setMapField(key, metadata);
      }
    }

    /*
     * This is the transition priority list, again the first inserted gets the top most priority.
     *
     */
    List<String> stateTransitionPriorityList = new ArrayList<String>();

    stateTransitionPriorityList.add("ONLINE-OFFLINE");
    stateTransitionPriorityList.add("ONLINE-DROPPED");
    stateTransitionPriorityList.add("OFFLINE-ONLINE");
    stateTransitionPriorityList.add("OFFLINE-DROPPED");

    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
        stateTransitionPriorityList);

    return new StateModelDefinition(record);
  }
}
