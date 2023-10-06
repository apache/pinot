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

import lombok.AllArgsConstructor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;


/**
 * Broker Resource layer state model to take over how to operate on:
 * Adding an external view to routing table.
 */
@AllArgsConstructor
public class BrokerResourceOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerResourceOnlineOfflineStateModelFactory.class);

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final HelixDataAccessor _helixDataAccessor;
  private final BrokerRoutingManager _routingManager;
  private final HelixExternalViewBasedQueryQuotaManager _queryQuotaManager;

  public static String getStateModelDef() {
    return "BrokerResourceOnlineOfflineStateModel";
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    return new BrokerResourceOnlineOfflineStateModel();
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public class BrokerResourceOnlineOfflineStateModel extends StateModel {

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      String tableNameWithType = message.getPartitionName();
      LOGGER.info("Processing transition from OFFLINE to ONLINE for table: {}", tableNameWithType);
      try {
        _routingManager.buildRouting(tableNameWithType);
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
        _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig,
            _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().externalView(BROKER_RESOURCE_INSTANCE)));
      } catch (Exception e) {
        LOGGER.error("Caught exception while processing transition from OFFLINE to ONLINE for table: {}",
            tableNameWithType, e);
        throw e;
      }
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      String tableNameWithType = message.getPartitionName();
      LOGGER.info("Processing transition from ONLINE to OFFLINE for table: {}", tableNameWithType);
      try {
        _routingManager.removeRouting(tableNameWithType);
        _queryQuotaManager.dropTableQueryQuota(tableNameWithType);
      } catch (Exception e) {
        LOGGER.error("Caught exception while processing transition from ONLINE to OFFLINE for table: {}",
            tableNameWithType, e);
        throw e;
      }
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.info("Transition from OFFLINE to DROPPED is no-op for table: {}", message.getPartitionName());
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      String tableNameWithType = message.getPartitionName();
      LOGGER.info("Processing transition from ONLINE to DROPPED for table: {}", tableNameWithType);
      try {
        _routingManager.removeRouting(tableNameWithType);
        _queryQuotaManager.dropTableQueryQuota(tableNameWithType);
      } catch (Exception e) {
        LOGGER.error("Caught exception while processing transition from ONLINE to DROPPED for table: {}",
            tableNameWithType, e);
        throw e;
      }
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      LOGGER.info("Transition from ERROR to OFFLINE is no-op for table: {}", message.getPartitionName());
    }
  }
}
