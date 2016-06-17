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
package com.linkedin.pinot.common.utils;

public class ControllerTenantNameBuilder {
  public final static String DEFAULT_TENANT_NAME = "DefaultTenant";

  private static String buildRealtimeTenantName(String tenantName) {
    return tenantName + "_" + ServerType.REALTIME.toString();
  }

  private static String buildOfflineTenantName(String tenantName) {
    return tenantName + "_" + ServerType.OFFLINE.toString();
  }

  private static String buildBrokerTenantName(String tenantName) {
    return tenantName + "_" + TenantRole.BROKER.toString();
  }

  public static TenantRole getTenantRoleFromTenantName(String tenantName) {
    if (tenantName.endsWith(ServerType.REALTIME.toString())) {
      return TenantRole.SERVER;
    }
    if (tenantName.endsWith(ServerType.OFFLINE.toString())) {
      return TenantRole.SERVER;
    }
    if (tenantName.endsWith(TenantRole.BROKER.toString())) {
      return TenantRole.BROKER;
    }
    throw new RuntimeException("Cannot identify tenant type from tenant name : " + tenantName);
  }

  public static String getTenantName(String tenantName, ServerType type) {
    if (type == ServerType.OFFLINE) {
      return getOfflineTenantNameForTenant(tenantName);
    }
    return getRealtimeTenantNameForTenant(tenantName);
  }

  public static String getRealtimeTenantNameForTenant(String tenantName) {
    if (tenantName == null) {
      return ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(DEFAULT_TENANT_NAME);
    }
    if (tenantName.endsWith(ServerType.REALTIME.toString())) {
      return tenantName;
    } else {
      return ControllerTenantNameBuilder.buildRealtimeTenantName(tenantName);
    }
  }

  public static String getOfflineTenantNameForTenant(String tenantName) {
    if (tenantName == null) {
      return ControllerTenantNameBuilder.getOfflineTenantNameForTenant(DEFAULT_TENANT_NAME);
    }
    if (tenantName.endsWith(ServerType.OFFLINE.toString())) {
      return tenantName;
    } else {
      return ControllerTenantNameBuilder.buildOfflineTenantName(tenantName);
    }
  }

  public static String getBrokerTenantNameForTenant(String tenantName) {
    if (tenantName == null) {
      return ControllerTenantNameBuilder.getBrokerTenantNameForTenant(DEFAULT_TENANT_NAME);
    }
    if (tenantName.endsWith(TenantRole.BROKER.toString())) {
      return tenantName;
    } else {
      return ControllerTenantNameBuilder.buildBrokerTenantName(tenantName);
    }
  }

  public static String getExternalTenantName(String tenantName) {
    if (tenantName.endsWith(ServerType.REALTIME.toString())) {
      return tenantName.substring(0, tenantName.length() - (ServerType.REALTIME.toString().length() + 1));
    }
    if (tenantName.endsWith(ServerType.OFFLINE.toString())) {
      return tenantName.substring(0, tenantName.length() - (ServerType.OFFLINE.toString().length() + 1));
    }
    if (tenantName.endsWith(TenantRole.BROKER.toString())) {
      return tenantName.substring(0, tenantName.length() - (TenantRole.BROKER.toString().length() + 1));
    }
    return tenantName;
  }

}
