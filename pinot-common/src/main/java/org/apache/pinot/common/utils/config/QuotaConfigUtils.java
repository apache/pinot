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
import org.apache.pinot.spi.config.tenant.TenantRole;


/// Resolves the effective quota of a table from its own [QuotaConfig] and the quota defaults of its tenants, and
/// manages the quota defaults of a tenant.
///
/// Each quota is inherited from the tenant whose role enforces it:
/// - storage (and its enforcement mode) from the table's server tenant, enforced by the controller
/// - query QPS (and its enforcement mode) from the table's broker tenant, enforced by the brokers
///
/// A table that sets a limit in its own quota config uses its own limit and enforcement mode, so tenant defaults never
/// weaken a limit set on the table (e.g. the storage limit of dimension tables). A table without a limit inherits the
/// tenant limit, with its own enforcement mode if set (so a table can opt out of a tenant limit by setting the
/// corresponding enforcement mode to DISABLED), otherwise the tenant enforcement mode.
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

  /// Returns the table's quota config with the storage quota resolved against the quota defaults of its server tenant,
  /// looked up with the given provider (which returns null when a tenant has no quota defaults). Query quota fields
  /// are the table's own.
  @Nullable
  public static QuotaConfig getEffectiveStorageQuotaConfig(TableConfig tableConfig,
      Function<String, QuotaConfig> tenantQuotaConfigProvider) {
    return mergeWithTenantQuotaConfigs(tableConfig, tenantQuotaConfigProvider.apply(getServerTenantName(tableConfig)),
        null);
  }

  /// Returns the table's quota config with the query quota resolved against the quota defaults of its broker tenant,
  /// looked up with the given provider (which returns null when a tenant has no quota defaults). Storage quota fields
  /// are the table's own.
  @Nullable
  public static QuotaConfig getEffectiveQueryQuotaConfig(TableConfig tableConfig,
      Function<String, QuotaConfig> tenantQuotaConfigProvider) {
    return mergeWithTenantQuotaConfigs(tableConfig, null,
        tenantQuotaConfigProvider.apply(getBrokerTenantName(tableConfig)));
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
    if (storage == null && serverTenantQuotaConfig != null && serverTenantQuotaConfig.getStorage() != null) {
      storage = serverTenantQuotaConfig.getStorage();
      storageQuotaEnforcementMode =
          firstNonNull(storageQuotaEnforcementMode, serverTenantQuotaConfig.getStorageQuotaEnforcementMode());
    }
    String maxQueriesPerSecond = tableQuotaConfig.getMaxQueriesPerSecond();
    QuotaEnforcementMode queryQuotaEnforcementMode = tableQuotaConfig.getQueryQuotaEnforcementMode();
    if (maxQueriesPerSecond == null && brokerTenantQuotaConfig != null
        && brokerTenantQuotaConfig.getMaxQueriesPerSecond() != null) {
      maxQueriesPerSecond = brokerTenantQuotaConfig.getMaxQueriesPerSecond();
      queryQuotaEnforcementMode =
          firstNonNull(queryQuotaEnforcementMode, brokerTenantQuotaConfig.getQueryQuotaEnforcementMode());
    }
    return new QuotaConfig(storage, maxQueriesPerSecond, storageQuotaEnforcementMode, queryQuotaEnforcementMode);
  }

  /// Validates a quota config to be set as the quota defaults of a tenant. Storage fields require the tenant to be a
  /// server tenant and query fields require it to be a broker tenant, since only those roles inherit them. An
  /// enforcement mode requires the corresponding limit, since a tenant enforcement mode only applies to the tables
  /// inheriting the tenant limit.
  ///
  /// @throws IllegalArgumentException if the quota config is not valid for the tenant
  public static void validateTenantQuotaConfig(QuotaConfig quotaConfig, boolean isServerTenant,
      boolean isBrokerTenant) {
    boolean hasStorageQuota = hasStorageQuota(quotaConfig);
    boolean hasQueryQuota = hasQueryQuota(quotaConfig);
    Preconditions.checkArgument(hasStorageQuota || hasQueryQuota,
        "Tenant quota config must set at least one of 'storage' and 'maxQueriesPerSecond'");
    Preconditions.checkArgument(
        quotaConfig.getStorageQuotaEnforcementMode() == null || quotaConfig.getStorage() != null,
        "'storageQuotaEnforcementMode' requires 'storage' to be set");
    Preconditions.checkArgument(
        quotaConfig.getQueryQuotaEnforcementMode() == null || quotaConfig.getMaxQueriesPerSecond() != null,
        "'queryQuotaEnforcementMode' requires 'maxQueriesPerSecond' to be set");
    Preconditions.checkArgument(!hasStorageQuota || isServerTenant,
        "Storage quota ('storage', 'storageQuotaEnforcementMode') can only be set on a server tenant");
    Preconditions.checkArgument(!hasQueryQuota || isBrokerTenant,
        "Query quota ('maxQueriesPerSecond', 'queryQuotaEnforcementMode') can only be set on a broker tenant");
  }

  /// Returns the quota defaults of a tenant after applying the given (validated) quota config, which only updates the
  /// roles it sets: the storage fields if it sets a storage quota, and the query fields if it sets a query quota. The
  /// fields of the other role are kept from the existing quota defaults. An enforcement mode that is not set is kept
  /// too, so that e.g. raising a limit in SHADOW mode does not start enforcing it; set the mode to ENFORCED to enforce.
  public static QuotaConfig replaceTenantQuotaConfigRoles(@Nullable QuotaConfig existingQuotaConfig,
      QuotaConfig quotaConfig) {
    if (existingQuotaConfig == null) {
      return quotaConfig;
    }
    String storage = existingQuotaConfig.getStorage();
    QuotaEnforcementMode storageQuotaEnforcementMode = existingQuotaConfig.getStorageQuotaEnforcementMode();
    if (hasStorageQuota(quotaConfig)) {
      storage = quotaConfig.getStorage();
      storageQuotaEnforcementMode =
          firstNonNull(quotaConfig.getStorageQuotaEnforcementMode(), storageQuotaEnforcementMode);
    }
    String maxQueriesPerSecond = existingQuotaConfig.getMaxQueriesPerSecond();
    QuotaEnforcementMode queryQuotaEnforcementMode = existingQuotaConfig.getQueryQuotaEnforcementMode();
    if (hasQueryQuota(quotaConfig)) {
      maxQueriesPerSecond = quotaConfig.getMaxQueriesPerSecond();
      queryQuotaEnforcementMode = firstNonNull(quotaConfig.getQueryQuotaEnforcementMode(), queryQuotaEnforcementMode);
    }
    return new QuotaConfig(storage, maxQueriesPerSecond, storageQuotaEnforcementMode, queryQuotaEnforcementMode);
  }

  /// Returns the quota defaults of a tenant after removing the fields of the given role: the storage fields for the
  /// server role and the query fields for the broker role.
  public static QuotaConfig removeTenantQuotaConfigRole(QuotaConfig quotaConfig, TenantRole tenantRole) {
    switch (tenantRole) {
      case SERVER:
        return new QuotaConfig(null, quotaConfig.getMaxQueriesPerSecond(), null,
            quotaConfig.getQueryQuotaEnforcementMode());
      case BROKER:
        return new QuotaConfig(quotaConfig.getStorage(), null, quotaConfig.getStorageQuotaEnforcementMode(), null);
      default:
        throw new IllegalArgumentException("Tenant quota defaults are not supported for tenant role: " + tenantRole);
    }
  }

  /// Returns whether the quota config sets any storage quota field.
  public static boolean hasStorageQuota(QuotaConfig quotaConfig) {
    return quotaConfig.getStorage() != null || quotaConfig.getStorageQuotaEnforcementMode() != null;
  }

  /// Returns whether the quota config sets any query quota field.
  public static boolean hasQueryQuota(QuotaConfig quotaConfig) {
    return quotaConfig.getMaxQueriesPerSecond() != null || quotaConfig.getQueryQuotaEnforcementMode() != null;
  }

  @Nullable
  private static <T> T firstNonNull(@Nullable T first, @Nullable T second) {
    return first != null ? first : second;
  }
}
