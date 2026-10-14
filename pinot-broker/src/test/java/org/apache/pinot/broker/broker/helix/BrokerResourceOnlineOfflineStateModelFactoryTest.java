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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.model.Message;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManager;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;


public class BrokerResourceOnlineOfflineStateModelFactoryTest {
  private static final String TABLE_NAME_WITH_TYPE = "testTable_OFFLINE";

  @Test
  @SuppressWarnings("unchecked")
  public void testRoutingIsRetainedOnlyWhileRequested() {
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    HelixExternalViewBasedQueryQuotaManager queryQuotaManager = mock(HelixExternalViewBasedQueryQuotaManager.class);
    AtomicBoolean retainRouting = new AtomicBoolean(true);
    BrokerResourceOnlineOfflineStateModelFactory factory =
        new BrokerResourceOnlineOfflineStateModelFactory(propertyStore, mock(HelixDataAccessor.class), routingManager,
            queryQuotaManager, null, retainRouting::get);
    BrokerResourceOnlineOfflineStateModelFactory.BrokerResourceOnlineOfflineStateModel stateModel =
        (BrokerResourceOnlineOfflineStateModelFactory.BrokerResourceOnlineOfflineStateModel)
            factory.createNewStateModel(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, TABLE_NAME_WITH_TYPE);
    Message message = new Message(Message.MessageType.STATE_TRANSITION, "testMessage");
    message.setPartitionName(TABLE_NAME_WITH_TYPE);

    // A draining broker keeps routing so the queries it accepted before drain can finish
    stateModel.onBecomeOfflineFromOnline(message, null);
    stateModel.onBecomeDroppedFromOnline(message, null);
    verifyNoInteractions(propertyStore, routingManager, queryQuotaManager);

    retainRouting.set(false);
    stateModel.onBecomeOfflineFromOnline(message, null);
    stateModel.onBecomeDroppedFromOnline(message, null);
    verify(routingManager, times(2)).removeRouting(TABLE_NAME_WITH_TYPE);
    verify(queryQuotaManager, times(2)).dropTableQueryQuota(TABLE_NAME_WITH_TYPE);
  }
}
