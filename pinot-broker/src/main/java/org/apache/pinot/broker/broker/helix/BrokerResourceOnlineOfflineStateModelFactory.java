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
package org.apache.pinot.broker.broker.helix;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManager;
import org.apache.pinot.broker.routing.HelixExternalViewBasedRouting;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;


/**
 * Broker Resource layer state model to take over how to operate on:
 * Adding an external view to routing table.
 *
 *
 */
public class BrokerResourceOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerResourceOnlineOfflineStateModelFactory.class);

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final HelixDataAccessor _helixDataAccessor;
  private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;
  private final HelixExternalViewBasedQueryQuotaManager _helixExternalViewBasedQueryQuotaManager;

  public BrokerResourceOnlineOfflineStateModelFactory(ZkHelixPropertyStore<ZNRecord> propertyStore,
      HelixDataAccessor helixDataAccessor, HelixExternalViewBasedRouting helixExternalViewBasedRouting,
      HelixExternalViewBasedQueryQuotaManager helixExternalViewBasedQueryQuotaManager) {
    _helixDataAccessor = helixDataAccessor;
    _propertyStore = propertyStore;
    _helixExternalViewBasedRouting = helixExternalViewBasedRouting;
    _helixExternalViewBasedQueryQuotaManager = helixExternalViewBasedQueryQuotaManager;
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
        String tableName = message.getPartitionName();
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableName);

        _helixExternalViewBasedRouting.markDataResourceOnline(tableConfig,
            _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().externalView(tableName)),
            _helixDataAccessor.getChildValues(_helixDataAccessor.keyBuilder().instanceConfigs()));
        _helixExternalViewBasedQueryQuotaManager.initTableQueryQuota(tableConfig,
            _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().externalView(BROKER_RESOURCE_INSTANCE)));
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
        _helixExternalViewBasedQueryQuotaManager.dropTableQueryQuota(tableName);
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
        _helixExternalViewBasedQueryQuotaManager.dropTableQueryQuota(tableName);
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
        _helixExternalViewBasedQueryQuotaManager.dropTableQueryQuota(tableName);
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
