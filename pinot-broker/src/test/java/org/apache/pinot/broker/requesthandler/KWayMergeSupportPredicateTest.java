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
package org.apache.pinot.broker.requesthandler;

import java.util.List;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class KWayMergeSupportPredicateTest {
  @Test
  public void testReleaseGateAndFinalization() {
    String version = "1.6.0";
    HelixManager manager = mock(HelixManager.class);
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);
    when(accessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster"));
    String broker = "Broker_localhost_8000";
    String server = "Server_localhost_9000";
    when(accessor.<LiveInstance>getChildValues(any(), eq(true)))
        .thenReturn(List.of(new LiveInstance(broker), new LiveInstance(server)));
    InstanceConfig brokerConfig = new InstanceConfig(broker);
    brokerConfig.getRecord().setSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, version);
    InstanceConfig serverConfig = new InstanceConfig(server);
    serverConfig.getRecord().setSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, version);
    KWayMergeSupportPredicate predicate = new KWayMergeSupportPredicate(manager, version);
    assertTrue(predicate.needWatchForVersionChanges());
    assertFalse(predicate.getAsBoolean());
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    predicate.onInstanceConfigChange(List.of(brokerConfig, serverConfig), context);
    clearInvocations(manager, accessor);
    assertTrue(predicate.getAsBoolean());
    verifyNoInteractions(manager, accessor);
    context.setType(NotificationContext.Type.FINALIZE);
    predicate.onLiveInstanceChange(List.of(), context);
    assertFalse(predicate.getAsBoolean());
  }

  @Test
  public void testSnapshotAndUnknownVersionsNeedNoListeners() {
    HelixManager manager = mock(HelixManager.class);
    for (String version : new String[]{null, PinotVersion.UNKNOWN, "1.6.0-SNAPSHOT"}) {
      KWayMergeSupportPredicate predicate = new KWayMergeSupportPredicate(manager, version);
      assertFalse(predicate.needWatchForVersionChanges());
      assertFalse(predicate.getAsBoolean());
    }
    verifyNoInteractions(manager);
  }
}
