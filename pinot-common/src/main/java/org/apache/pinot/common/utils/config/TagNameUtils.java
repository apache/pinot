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
package org.apache.pinot.common.utils.config;

import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.tenant.TenantRole;


/**
 * The <code>TagNameUtils</code> class handles the conversion between tenant and tag. For single-tenant cluster, use
 * {@link #DEFAULT_TENANT_NAME} ("DefaultTenant") as the tenant name.
 * <p>Example:
 * <ul>
 *   <li>Tenant name: myTenant</li>
 *   <li>Broker tag name: myTenant_BROKER</li>
 *   <li>Offline server tag name: myTenant_OFFLINE</li>
 *   <li>Realtime server tag name: myTenant_REALTIME</li>
 * </ul>
 * Tag name can be overridden by {@link TagOverrideConfig} from {@link TenantConfig}.
 */
public class TagNameUtils {
  private TagNameUtils() {
  }

  public final static String DEFAULT_TENANT_NAME = "DefaultTenant";
  private final static String BROKER_TAG_SUFFIX = "_" + TenantRole.BROKER;
  private final static String OFFLINE_SERVER_TAG_SUFFIX = "_" + TableType.OFFLINE;
  private final static String REALTIME_SERVER_TAG_SUFFIX = "_" + TableType.REALTIME;

  /**
   * Returns whether the given tag is a broker tag.
   */
  public static boolean isBrokerTag(String tagName) {
    return tagName.endsWith(BROKER_TAG_SUFFIX);
  }

  /**
   * Returns whether the given tag is a server tag (OFFLINE or REALTIME).
   */
  public static boolean isServerTag(String tagName) {
    return isOfflineServerTag(tagName) || isRealtimeServerTag(tagName);
  }

  /**
   * Returns whether the given tag is an OFFLINE server tag.
   */
  public static boolean isOfflineServerTag(String tagName) {
    return tagName.endsWith(OFFLINE_SERVER_TAG_SUFFIX);
  }

  /**
   * Returns whether the given tag is an REALTIME server tag.
   */
  public static boolean isRealtimeServerTag(String tagName) {
    return tagName.endsWith(REALTIME_SERVER_TAG_SUFFIX);
  }

  /**
   * Returns the tenant name for the given tag. The given tag should be valid (with proper tag suffix).
   */
  public static String getTenantFromTag(String tagName) {
    return tagName.substring(0, tagName.lastIndexOf('_'));
  }

  /**
   * Returns the Broker tag name for the given tenant.
   */
  public static String getBrokerTagForTenant(@Nullable String tenantName) {
    return getTagForTenant(tenantName, BROKER_TAG_SUFFIX);
  }

  /**
   * Returns the OFFLINE server tag name for the given tenant.
   */
  public static String getOfflineTagForTenant(@Nullable String tenantName) {
    return getTagForTenant(tenantName, OFFLINE_SERVER_TAG_SUFFIX);
  }

  /**
   * Returns the REALTIME server tag name for the given tenant.
   */
  public static String getRealtimeTagForTenant(@Nullable String tenantName) {
    return getTagForTenant(tenantName, REALTIME_SERVER_TAG_SUFFIX);
  }

  /**
   * Returns the server tag name for the given tenant and the given table type.
   */
  public static String getServerTagForTenant(@Nullable String tenantName, TableType type) {
    return getTagForTenant(tenantName, String.format("_%s", type));
  }

  private static String getTagForTenant(@Nullable String tenantName, String tagSuffix) {
    if (tenantName == null) {
      return DEFAULT_TENANT_NAME + tagSuffix;
    } else {
      return tenantName + tagSuffix;
    }
  }

  /**
   * Extracts the broker tag name from the given tenant config.
   */
  public static String extractBrokerTag(TenantConfig tenantConfig) {
    return getBrokerTagForTenant(tenantConfig.getBroker());
  }

  /**
   * Extracts the OFFLINE server tag name from the given tenant config.
   */
  public static String extractOfflineServerTag(TenantConfig tenantConfig) {
    return getOfflineTagForTenant(tenantConfig.getServer());
  }

  /**
   * Extracts the REALTIME server tag name from the given tenant config.
   */
  public static String extractRealtimeServerTag(TenantConfig tenantConfig) {
    return getRealtimeTagForTenant(tenantConfig.getServer());
  }

  /**
   * Extracts the REALTIME consuming server tag name from the given tenant config.
   */
  public static String extractConsumingServerTag(TenantConfig tenantConfig) {
    TagOverrideConfig tagOverrideConfig = tenantConfig.getTagOverrideConfig();
    if (tagOverrideConfig == null || tagOverrideConfig.getRealtimeConsuming() == null) {
      return getRealtimeTagForTenant(tenantConfig.getServer());
    } else {
      return tagOverrideConfig.getRealtimeConsuming();
    }
  }

  /**
   * Extracts the REALTIME completed server tag name from the given tenant config.
   */
  public static String extractCompletedServerTag(TenantConfig tenantConfig) {
    TagOverrideConfig tagOverrideConfig = tenantConfig.getTagOverrideConfig();
    if (tagOverrideConfig == null || tagOverrideConfig.getRealtimeCompleted() == null) {
      return getRealtimeTagForTenant(tenantConfig.getServer());
    } else {
      return tagOverrideConfig.getRealtimeCompleted();
    }
  }

  /**
   * Returns whether the completed segments need to be relocated (completed server tag is different from consuming
   * server tag).
   */
  public static boolean isRelocateCompletedSegments(TenantConfig tenantConfig) {
    if (tenantConfig.getTagOverrideConfig() == null) {
      return false;
    } else {
      return !extractConsumingServerTag(tenantConfig).equals(extractCompletedServerTag(tenantConfig));
    }
  }
}
