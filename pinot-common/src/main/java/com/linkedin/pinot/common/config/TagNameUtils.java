/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.utils.ServerType;
import com.linkedin.pinot.common.utils.TenantRole;


public class TagNameUtils {
  public final static String DEFAULT_TENANT_NAME = "DefaultTenant";

  private static String buildRealtimeTagFromTenantName(String tenantName) {
    return tenantName + "_" + ServerType.REALTIME.toString();
  }

  private static String buildOfflineTagFromTenantName(String tenantName) {
    return tenantName + "_" + ServerType.OFFLINE.toString();
  }

  private static String buildBrokerTenantTagFromTenantName(String tenantName) {
    return tenantName + "_" + TenantRole.BROKER.toString();
  }

  public static boolean hasValidServerTagSuffix(String tagName) {
    if (tagName.endsWith(ServerType.REALTIME.toString()) || tagName.endsWith(ServerType.OFFLINE.toString())) {
      return true;
    }
    return false;
  }

  public static TenantRole getTenantRoleFromTag(String tagName) {
    if (tagName.endsWith(ServerType.REALTIME.toString())) {
      return TenantRole.SERVER;
    }
    if (tagName.endsWith(ServerType.OFFLINE.toString())) {
      return TenantRole.SERVER;
    }
    if (tagName.endsWith(TenantRole.BROKER.toString())) {
      return TenantRole.BROKER;
    }
    throw new RuntimeException("Cannot identify tenant type from tag name : " + tagName);
  }

  public static String getTagFromTenantAndServerType(String tenantName, ServerType type) {
    if (type == ServerType.OFFLINE) {
      return getOfflineTagForTenant(tenantName);
    }
    return getRealtimeTagForTenant(tenantName);
  }

  public static String getRealtimeTagForTenant(String tenantName) {
    if (tenantName == null) {
      return TagNameUtils.getRealtimeTagForTenant(DEFAULT_TENANT_NAME);
    }
    if (tenantName.endsWith(ServerType.REALTIME.toString())) {
      return tenantName;
    } else {
      return TagNameUtils.buildRealtimeTagFromTenantName(tenantName);
    }
  }

  public static String getOfflineTagForTenant(String tenantName) {
    if (tenantName == null) {
      return TagNameUtils.getOfflineTagForTenant(DEFAULT_TENANT_NAME);
    }
    if (tenantName.endsWith(ServerType.OFFLINE.toString())) {
      return tenantName;
    } else {
      return TagNameUtils.buildOfflineTagFromTenantName(tenantName);
    }
  }

  public static String getBrokerTagForTenant(String tenantName) {
    if (tenantName == null) {
      return TagNameUtils.getBrokerTagForTenant(DEFAULT_TENANT_NAME);
    }
    if (tenantName.endsWith(TenantRole.BROKER.toString())) {
      return tenantName;
    } else {
      return TagNameUtils.buildBrokerTenantTagFromTenantName(tenantName);
    }
  }

  public static String getTenantNameFromTag(String tag) {
    if (tag.endsWith(ServerType.REALTIME.toString())) {
      return tag.substring(0, tag.length() - (ServerType.REALTIME.toString().length() + 1));
    }
    if (tag.endsWith(ServerType.OFFLINE.toString())) {
      return tag.substring(0, tag.length() - (ServerType.OFFLINE.toString().length() + 1));
    }
    if (tag.endsWith(TenantRole.BROKER.toString())) {
      return tag.substring(0, tag.length() - (TenantRole.BROKER.toString().length() + 1));
    }
    return tag;
  }

}
