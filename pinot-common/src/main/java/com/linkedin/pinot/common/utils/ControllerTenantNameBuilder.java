/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
  private static final String REALTIME_TENANT_SUFFIX = "_REALTIME";
  private static final String OFFLINE_TENANT_SUFFIX = "_OFFLINE";
  private static final String BROKER_TENANT_SUFFIX = "_BROKER";

  private static String buildRealtimeTenantName(String tenantName) {
    return tenantName + REALTIME_TENANT_SUFFIX;
  }

  private static String buildOfflineTenantName(String tenantName) {
    return tenantName + OFFLINE_TENANT_SUFFIX;
  }

  private static String buildBrokerTenantName(String tenantName) {
    return tenantName + BROKER_TENANT_SUFFIX;
  }

  public static TenantRole getTenantRoleFromTenantName(String tenantName) {
    if (tenantName.endsWith(REALTIME_TENANT_SUFFIX)) {
      return TenantRole.SERVER;
    }
    if (tenantName.endsWith(OFFLINE_TENANT_SUFFIX)) {
      return TenantRole.SERVER;
    }
    if (tenantName.endsWith(BROKER_TENANT_SUFFIX)) {
      return TenantRole.BROKER;
    }
    throw new RuntimeException("Cannot identify tenant type from tenant name : " + tenantName);
  }

  public static String getRealtimeTenantNameForTenant(String tenantName) {
    if (tenantName.endsWith(REALTIME_TENANT_SUFFIX)) {
      return tenantName;
    } else {
      return ControllerTenantNameBuilder.buildRealtimeTenantName(tenantName);
    }
  }

  public static String getOfflineTenantNameForTenant(String tenantName) {
    if (tenantName.endsWith(OFFLINE_TENANT_SUFFIX)) {
      return tenantName;
    } else {
      return ControllerTenantNameBuilder.buildOfflineTenantName(tenantName);
    }
  }

  public static String getBrokerTenantNameForTenant(String tenantName) {
    if (tenantName.endsWith(BROKER_TENANT_SUFFIX)) {
      return tenantName;
    } else {
      return ControllerTenantNameBuilder.buildBrokerTenantName(tenantName);
    }
  }

  public static String getExternalTenantName(String tenantName) {
    if (tenantName.endsWith(REALTIME_TENANT_SUFFIX)) {
      return tenantName.substring(0, tenantName.length() - REALTIME_TENANT_SUFFIX.length());
    }
    if (tenantName.endsWith(OFFLINE_TENANT_SUFFIX)) {
      return tenantName.substring(0, tenantName.length() - OFFLINE_TENANT_SUFFIX.length());
    }
    if (tenantName.endsWith(BROKER_TENANT_SUFFIX)) {
      return tenantName.substring(0, tenantName.length() - BROKER_TENANT_SUFFIX.length());
    }
    return tenantName;
  }
}
