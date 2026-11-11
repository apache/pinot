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

import com.google.common.base.Preconditions;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.QuotaEnforcementMode;
import org.apache.pinot.spi.config.table.TableConfig;


/// Resolves the effective quota of a table from its own [QuotaConfig] and the quota defaults of its tenants.
///
/// Each quota is inherited from the tenant whose role enforces it:
/// - storage (and its enforcement mode) from the table's server tenant, enforced by the controller
/// - query QPS (and its enforcement mode) from the table's broker tenant, enforced by the brokers
///
/// Resolution is per field: a field set in the table quota config wins, otherwise the tenant default applies. Each
/// enforcement mode is resolved independently of its limit, so a tenant can e.g. put all its tables in SHADOW mode
/// while tables still set their own limits, and a table can opt out of a tenant limit by setting the corresponding
/// enforcement mode to DISABLED.
///
/// Stateless and thread-safe.
public class QuotaConfigUtils {
  private QuotaConfigUtils() {
  }

  /// Returns the server tenant whose storage quota defaults apply to the table, which is the server tenant configured
  /// in the table's tenant config, or the default tenant when none is configured. Tag overrides are not considered.
  public static String getServerTenantName(TableConfig tableConfig) {
    String serverTenant = tableConfig.getTenantConfig().getServer();
    return serverTenant != null ? serverTenant : TagNameUtils.DEFAULT_TENANT_NAME;
  }

  /// Returns the broker tenant whose query quota defaults apply to the table, which is the broker tenant configured in
  /// the table's tenant config, or the default tenant when none is configured.
  public static String getBrokerTenantName(TableConfig tableConfig) {
    String brokerTenant = tableConfig.getTenantConfig().getBroker();
    return brokerTenant != null ? brokerTenant : TagNameUtils.DEFAULT_TENANT_NAME;
  }

  /// Returns the effective quota config of the table, looking up the quota defaults of its server and broker tenants
  /// with the given provider (which returns null when a tenant has no quota defaults).
  @Nullable
  public static QuotaConfig getEffectiveQuotaConfig(TableConfig tableConfig,
      Function<String, QuotaConfig> tenantQuotaConfigProvider) {
    String serverTenant = getServerTenantName(tableConfig);
    String brokerTenant = getBrokerTenantName(tableConfig);
    QuotaConfig serverTenantQuotaConfig = tenantQuotaConfigProvider.apply(serverTenant);
    QuotaConfig brokerTenantQuotaConfig =
        brokerTenant.equals(serverTenant) ? serverTenantQuotaConfig : tenantQuotaConfigProvider.apply(brokerTenant);
    return mergeWithTenantQuotaConfigs(tableConfig, serverTenantQuotaConfig, brokerTenantQuotaConfig);
  }

  /// Returns the effective quota config of the table given the quota defaults of its server and broker tenants.
  /// Returns the table's own quota config as is when neither tenant has quota defaults.
  @Nullable
  public static QuotaConfig mergeWithTenantQuotaConfigs(TableConfig tableConfig,
      @Nullable QuotaConfig serverTenantQuotaConfig, @Nullable QuotaConfig brokerTenantQuotaConfig) {
    QuotaConfig tableQuotaConfig = tableConfig.getQuotaConfig();
    if (serverTenantQuotaConfig == null && brokerTenantQuotaConfig == null) {
      return tableQuotaConfig;
    }
    if (tableQuotaConfig == null) {
      tableQuotaConfig = new QuotaConfig(null, null);
    }
    String storage = tableQuotaConfig.getStorage();
    QuotaEnforcementMode storageQuotaEnforcementMode = tableQuotaConfig.getStorageQuotaEnforcementMode();
    if (serverTenantQuotaConfig != null) {
      storage = firstNonNull(storage, serverTenantQuotaConfig.getStorage());
      storageQuotaEnforcementMode =
          firstNonNull(storageQuotaEnforcementMode, serverTenantQuotaConfig.getStorageQuotaEnforcementMode());
    }
    String maxQueriesPerSecond = tableQuotaConfig.getMaxQueriesPerSecond();
    QuotaEnforcementMode queryQuotaEnforcementMode = tableQuotaConfig.getQueryQuotaEnforcementMode();
    if (brokerTenantQuotaConfig != null) {
      maxQueriesPerSecond = firstNonNull(maxQueriesPerSecond, brokerTenantQuotaConfig.getMaxQueriesPerSecond());
      queryQuotaEnforcementMode =
          firstNonNull(queryQuotaEnforcementMode, brokerTenantQuotaConfig.getQueryQuotaEnforcementMode());
    }
    return new QuotaConfig(storage, maxQueriesPerSecond, storageQuotaEnforcementMode, queryQuotaEnforcementMode);
  }

  /// Validates a quota config to be set as the quota defaults of a tenant. Storage fields require the tenant to be a
  /// server tenant and query fields require it to be a broker tenant, since only those roles inherit them.
  ///
  /// @throws IllegalArgumentException if the quota config is not valid for the tenant
  public static void validateTenantQuotaConfig(QuotaConfig quotaConfig, boolean isServerTenant,
      boolean isBrokerTenant) {
    boolean hasStorageFields =
        quotaConfig.getStorage() != null || quotaConfig.getStorageQuotaEnforcementMode() != null;
    boolean hasQueryFields =
        quotaConfig.getMaxQueriesPerSecond() != null || quotaConfig.getQueryQuotaEnforcementMode() != null;
    Preconditions.checkArgument(hasStorageFields || hasQueryFields,
        "Tenant quota config must set at least one of 'storage', 'storageQuotaEnforcementMode', "
            + "'maxQueriesPerSecond' and 'queryQuotaEnforcementMode'");
    Preconditions.checkArgument(!hasStorageFields || isServerTenant,
        "Storage quota ('storage', 'storageQuotaEnforcementMode') can only be set on a server tenant");
    Preconditions.checkArgument(!hasQueryFields || isBrokerTenant,
        "Query quota ('maxQueriesPerSecond', 'queryQuotaEnforcementMode') can only be set on a broker tenant");
  }

  @Nullable
  private static <T> T firstNonNull(@Nullable T first, @Nullable T second) {
    return first != null ? first : second;
  }
}
