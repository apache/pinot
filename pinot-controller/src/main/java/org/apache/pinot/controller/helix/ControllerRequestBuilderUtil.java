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
package org.apache.pinot.controller.helix;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.config.Tenant;
import org.apache.pinot.common.config.Tenant.TenantBuilder;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.TenantRole;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;

import static org.apache.pinot.common.utils.CommonConstants.Helix.*;


public class ControllerRequestBuilderUtil {
  private ControllerRequestBuilderUtil() {
  }

  public static void addFakeBrokerInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances) throws Exception {
    addFakeBrokerInstancesToAutoJoinHelixCluster(helixClusterName, zkServer, numInstances, false);
  }

  public static void addFakeBrokerInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances, boolean isSingleTenant) throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      final String brokerId = "Broker_localhost_" + i;
      final HelixManager helixZkManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.PARTICIPANT, zkServer);
      final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
      final StateModelFactory<?> stateModelFactory = new EmptyBrokerOnlineOfflineStateModelFactory();
      stateMachineEngine.registerStateModelFactory(EmptyBrokerOnlineOfflineStateModelFactory.getStateModelDef(),
          stateModelFactory);
      helixZkManager.connect();
      if (isSingleTenant) {
        helixZkManager.getClusterManagmentTool()
            .addInstanceTag(helixClusterName, brokerId,
                TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME));
      } else {
        helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, brokerId, UNTAGGED_BROKER_INSTANCE);
      }
    }
  }

  public static void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances) throws Exception {
    addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, zkServer, numInstances, false,
        CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
  }

  public static void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances, boolean isSingleTenant) throws Exception {
    addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, zkServer, numInstances, isSingleTenant,
        CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
  }

  public static void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances, boolean isSingleTenant, int adminPort) throws Exception {

    for (int i = 0; i < numInstances; ++i) {
      final String instanceId = "Server_localhost_" + i;
      addFakeDataInstanceToAutoJoinHelixCluster(helixClusterName, zkServer, instanceId, isSingleTenant, adminPort + i);
    }
  }

  public static void addFakeDataInstanceToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      String instanceId) throws Exception {
    addFakeDataInstanceToAutoJoinHelixCluster(helixClusterName, zkServer, instanceId, false,
        CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
  }

  public static void addFakeDataInstanceToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      String instanceId, boolean isSingleTenant) throws Exception {
    addFakeDataInstanceToAutoJoinHelixCluster(helixClusterName, zkServer, instanceId, isSingleTenant,
        CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
  }

  public static void addFakeDataInstanceToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      String instanceId, boolean isSingleTenant, int adminPort) throws Exception {
    final HelixManager helixZkManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer);
    final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
    final StateModelFactory<?> stateModelFactory = new EmptySegmentOnlineOfflineStateModelFactory();
    stateMachineEngine.registerStateModelFactory(EmptySegmentOnlineOfflineStateModelFactory.getStateModelDef(),
        stateModelFactory);
    helixZkManager.connect();
    if (isSingleTenant) {
      helixZkManager.getClusterManagmentTool()
          .addInstanceTag(helixClusterName, instanceId,
              TableNameBuilder.OFFLINE.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));
      helixZkManager.getClusterManagmentTool()
          .addInstanceTag(helixClusterName, instanceId,
              TableNameBuilder.REALTIME.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));
    } else {
      helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId, UNTAGGED_SERVER_INSTANCE);
    }
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT, helixClusterName).forParticipant(
            instanceId).build();
    Map<String, String> props = new HashMap<>();
    props.put(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, String.valueOf(adminPort));
    helixZkManager.getClusterManagmentTool().setConfig(scope, props);
  }

  public static String buildBrokerTenantCreateRequestJSON(String tenantName, int numberOfInstances)
      throws JsonProcessingException {
    Tenant tenant = new TenantBuilder(tenantName).setRole(TenantRole.BROKER)
        .setTotalInstances(numberOfInstances)
        .setOfflineInstances(0)
        .setRealtimeInstances(0)
        .build();
    return JsonUtils.objectToString(tenant);
  }

  public static String buildServerTenantCreateRequestJSON(String tenantName, int numberOfInstances,
      int offlineInstances, int realtimeInstances) throws JsonProcessingException {
    Tenant tenant = new TenantBuilder(tenantName).setRole(TenantRole.SERVER)
        .setTotalInstances(numberOfInstances)
        .setOfflineInstances(offlineInstances)
        .setRealtimeInstances(realtimeInstances)
        .build();
    return JsonUtils.objectToString(tenant);
  }
}
