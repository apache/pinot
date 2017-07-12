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
package com.linkedin.pinot.controller.helix;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.config.Tenant.TenantBuilder;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.TenantRole;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.json.JSONException;
import org.json.JSONObject;

import static com.linkedin.pinot.common.utils.CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE;
import static com.linkedin.pinot.common.utils.CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE;


public class ControllerRequestBuilderUtil {
  private ControllerRequestBuilderUtil() {
  }

  public static void addFakeBrokerInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances)
      throws Exception {
    addFakeBrokerInstancesToAutoJoinHelixCluster(helixClusterName, zkServer, numInstances, false);
  }

  public static void addFakeBrokerInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances, boolean isSingleTenant)
      throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      final String brokerId = "Broker_localhost_" + i;
      final HelixManager helixZkManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.PARTICIPANT, zkServer);
      final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
      final StateModelFactory<?> stateModelFactory = new EmptyBrokerOnlineOfflineStateModelFactory();
      stateMachineEngine
          .registerStateModelFactory(EmptyBrokerOnlineOfflineStateModelFactory.getStateModelDef(), stateModelFactory);
      helixZkManager.connect();
      if (isSingleTenant) {
        helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, brokerId,
            ControllerTenantNameBuilder.getBrokerTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
      } else {
        helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, brokerId, UNTAGGED_BROKER_INSTANCE);
      }
    }
  }

  public static void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances)
      throws Exception {
    addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, zkServer, numInstances, false, 8097);
  }

  public static void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances, boolean isSingleTenant)
      throws Exception {
    addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, zkServer, numInstances, isSingleTenant, 8097);
  }

  public static void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances, boolean isSingleTenant, int adminPort)
      throws Exception {

    for (int i = 0; i < numInstances; ++i) {
      final String instanceId = "Server_localhost_" + i;

      final HelixManager helixZkManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer);
      final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
      final StateModelFactory<?> stateModelFactory = new EmptySegmentOnlineOfflineStateModelFactory();
      stateMachineEngine
          .registerStateModelFactory(EmptySegmentOnlineOfflineStateModelFactory.getStateModelDef(), stateModelFactory);
      helixZkManager.connect();
      if (isSingleTenant) {
        helixZkManager.getClusterManagmentTool()
            .addInstanceTag(helixClusterName, instanceId,
                TableNameBuilder.OFFLINE.tableNameWithType(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
        helixZkManager.getClusterManagmentTool()
            .addInstanceTag(helixClusterName, instanceId,
                TableNameBuilder.REALTIME.tableNameWithType(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
      } else {
        helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId, UNTAGGED_SERVER_INSTANCE);
      }
      HelixConfigScope scope =
          new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT, helixClusterName)
              .forParticipant(instanceId).build();
      Map<String, String> props = new HashMap<>();
      props.put(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, String.valueOf(adminPort + i));
      helixZkManager.getClusterManagmentTool().setConfig(scope, props);
    }
  }

  public static JSONObject buildBrokerTenantCreateRequestJSON(String tenantName, int numberOfInstances)
      throws JSONException {
    Tenant tenant = new TenantBuilder(tenantName).setRole(TenantRole.BROKER).setTotalInstances(numberOfInstances)
        .setOfflineInstances(0).setRealtimeInstances(0).build();
    return tenant.toJSON();
  }

  public static JSONObject buildServerTenantCreateRequestJSON(String tenantName, int numberOfInstances,
      int offlineInstances, int realtimeInstances)
      throws JSONException {
    Tenant tenant = new TenantBuilder(tenantName).setRole(TenantRole.SERVER).setTotalInstances(numberOfInstances)
        .setOfflineInstances(offlineInstances).setRealtimeInstances(realtimeInstances).build();
    return tenant.toJSON();
  }
}
