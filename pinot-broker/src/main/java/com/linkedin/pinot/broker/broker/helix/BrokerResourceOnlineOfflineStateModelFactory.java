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
package com.linkedin.pinot.broker.broker.helix;

import java.util.List;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;


/**
 * Broker Resource layer state model to take over how to operate on:
 * Adding an external view to routing table.
 *
 *
 */
public class BrokerResourceOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {

  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private HelixExternalViewBasedRouting _helixExternalViewBasedRouting;

  public BrokerResourceOnlineOfflineStateModelFactory(HelixManager helixManager,
      HelixExternalViewBasedRouting helixExternalViewBasedRouting) {
    _helixManager = helixManager;
    _helixExternalViewBasedRouting = helixExternalViewBasedRouting;
  }

  public static String getStateModelDef() {
    return "BrokerResourceOnlineOfflineStateModel";
  }

  @Override
  public StateModel createNewStateModel(String resourceName) {
    return new BrokerResourceOnlineOfflineStateModel();
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public class BrokerResourceOnlineOfflineStateModel extends StateModel {
    private final Logger LOGGER = LoggerFactory.getLogger(BrokerResourceOnlineOfflineStateModelFactory.class);

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      ensureHelixAdminIsInitialized();

      try {
        LOGGER.info("BrokerResourceOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
        Builder keyBuilder = _helixManager.getHelixDataAccessor().keyBuilder();
        String resourceName = message.getPartitionName();
        HelixDataAccessor helixDataAccessor = _helixManager.getHelixDataAccessor();
        List<InstanceConfig> instanceConfigList = helixDataAccessor.getChildValues(keyBuilder.instanceConfigs());

        _helixExternalViewBasedRouting.markDataResourceOnline(
            resourceName,
            HelixHelper.getExternalViewForResource(_helixAdmin, _helixManager.getClusterName(), resourceName),
            instanceConfigList);
      } catch (Exception e) {
        LOGGER.error("Caught exception during OFFLINE -> ONLINE transition", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      ensureHelixAdminIsInitialized();

      try {
        LOGGER.info("BrokerResourceOnlineOfflineStateModel.onBecomeOfflineFromOnline() : " + message);
        String resourceName = message.getResourceName();
        _helixExternalViewBasedRouting.markDataResourceOffline(resourceName);
      } catch (Exception e) {
        LOGGER.error("Caught exception during ONLINE -> OFFLINE transition", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      ensureHelixAdminIsInitialized();

      try {
        LOGGER.info("BrokerResourceOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);
        String resourceName = message.getResourceName();
        _helixExternalViewBasedRouting.markDataResourceOffline(resourceName);
      } catch (Exception e) {
        LOGGER.error("Caught exception during OFFLINE -> DROPPED transition", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      ensureHelixAdminIsInitialized();

      try {
        LOGGER.info("BrokerResourceOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);
        String resourceName = message.getResourceName();
        _helixExternalViewBasedRouting.markDataResourceOffline(resourceName);
      } catch (Exception e) {
        LOGGER.error("Caught exception during ONLINE -> DROPPED transition", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      ensureHelixAdminIsInitialized();

      LOGGER.info("Resetting the state for broker resource:{} from ERROR to OFFLINE", message.getPartitionName());
    }

  }

  private void ensureHelixAdminIsInitialized() {
    if (_helixAdmin == null) {
      _helixAdmin = _helixManager.getClusterManagmentTool();
    }
  }
}
