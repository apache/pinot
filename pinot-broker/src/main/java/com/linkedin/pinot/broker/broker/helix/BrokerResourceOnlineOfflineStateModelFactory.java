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

import com.linkedin.pinot.broker.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.helix.HelixHelper;

import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Broker Resource layer state model to take over how to operate on:
 * Adding an external view to routing table.
 *
 *
 */
public class BrokerResourceOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerResourceOnlineOfflineStateModelFactory.class);

  private final HelixManager _helixManager;
  private final HelixAdmin _helixAdmin;
  private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public BrokerResourceOnlineOfflineStateModelFactory(HelixManager helixManager,
      ZkHelixPropertyStore<ZNRecord> propertyStore, HelixExternalViewBasedRouting helixExternalViewBasedRouting) {
    _helixManager = helixManager;
    _propertyStore = propertyStore;
    _helixAdmin = helixManager.getClusterManagmentTool();
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

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      try {
        LOGGER.info("BrokerResourceOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
        Builder keyBuilder = _helixManager.getHelixDataAccessor().keyBuilder();
        String tableName = message.getPartitionName();
        HelixDataAccessor helixDataAccessor = _helixManager.getHelixDataAccessor();
        List<InstanceConfig> instanceConfigList = helixDataAccessor.getChildValues(keyBuilder.instanceConfigs());
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableName);

        _helixExternalViewBasedRouting.markDataResourceOnline(
            tableConfig,
            HelixHelper.getExternalViewForResource(_helixAdmin, _helixManager.getClusterName(), tableName),
            instanceConfigList);
      } catch (Exception e) {
        LOGGER.error("Caught exception during OFFLINE -> ONLINE transition", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      try {
        LOGGER.info("BrokerResourceOnlineOfflineStateModel.onBecomeOfflineFromOnline() : " + message);
        String tableName = message.getPartitionName();
        _helixExternalViewBasedRouting.markDataResourceOffline(tableName);
      } catch (Exception e) {
        LOGGER.error("Caught exception during ONLINE -> OFFLINE transition", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      try {
        LOGGER.info("BrokerResourceOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);
        String tableName = message.getPartitionName();
        _helixExternalViewBasedRouting.markDataResourceOffline(tableName);
      } catch (Exception e) {
        LOGGER.error("Caught exception during OFFLINE -> DROPPED transition", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      try {
        LOGGER.info("BrokerResourceOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);
        String tableName = message.getPartitionName();
        _helixExternalViewBasedRouting.markDataResourceOffline(tableName);
      } catch (Exception e) {
        LOGGER.error("Caught exception during ONLINE -> DROPPED transition", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      LOGGER.info("Resetting the state for broker resource:{} from ERROR to OFFLINE", message.getPartitionName());
    }
  }
}
