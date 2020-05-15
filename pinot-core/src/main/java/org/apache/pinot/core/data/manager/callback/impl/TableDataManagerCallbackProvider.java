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
package org.apache.pinot.core.data.manager.callback.impl;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.core.data.manager.callback.TableDataManagerCallback;
import org.apache.pinot.core.data.manager.config.TableDataManagerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * class for creating appropriate {@link TableDataManagerCallback} depends on the config
 * allow upsert-enabled pinot server to inject proper logic while keeping append-only pinot server keep the same
 */
public class TableDataManagerCallbackProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableDataManagerCallbackProvider.class);

  private Class<TableDataManagerCallback> defaultTableDataManagerCallBackClass;
  private Class<TableDataManagerCallback> upsertTableDataManagerCallBackClass;

  public static final String UPSERT_CALLBACK_CLASS_CONFIG_KEY = "upsert.tableDataManager.callback";
  public static final String DEFAULT_CALLBACK_CLASS_CONFIG_KEY = "append.tableDataManager.callback";
  public static final String CALLBACK_CLASS_CONFIG_DEFAULT = DefaultTableDataManagerCallbackImpl.class.getName();

  /**
   * Class to initialize table data manager callback provider
   * Pinot will extract callback runtime configuration from config key {@value UPSERT_CALLBACK_CLASS_CONFIG_KEY}
   * to create proper callback class for upsert-enabled pinot server environment
   * @param configuration the configuration for the pinot upsert components
   */
  public TableDataManagerCallbackProvider(Configuration configuration) {
    String appendClassName = configuration.getString(DEFAULT_CALLBACK_CLASS_CONFIG_KEY, CALLBACK_CLASS_CONFIG_DEFAULT);
    String upsertClassName = configuration.getString(UPSERT_CALLBACK_CLASS_CONFIG_KEY);
    try {
      defaultTableDataManagerCallBackClass = (Class<TableDataManagerCallback>) Class.forName(appendClassName);
    } catch (ClassNotFoundException e) {
      LOGGER.error("failed to load table data manager class {}", appendClassName, e);
      ExceptionUtils.rethrow(e);
    }
    if (StringUtils.isNotEmpty(upsertClassName)) {
      try {
        upsertTableDataManagerCallBackClass = (Class<TableDataManagerCallback>) Class.forName(upsertClassName);
      } catch (ClassNotFoundException e) {
        LOGGER.error("failed to load table data manager class {}", upsertClassName);
        ExceptionUtils.rethrow(e);
      }
      Preconditions.checkState(upsertTableDataManagerCallBackClass.isAssignableFrom(TableDataManagerCallback.class),
          "configured class not assignable from Callback class");
    }
  }

  /**
   * create a proper callback for the table, depends on whether the table is configured for upsert or not
   * @param tableDataManagerConfig the config for the table
   */
  public TableDataManagerCallback getTableDataManagerCallback(TableDataManagerConfig tableDataManagerConfig) {
    if (tableDataManagerConfig.isTableForUpsert()) {
      return getUpsertTableDataManagerCallback();
    } else {
      return getDefaultTableDataManagerCallback();
    }
  }

  private TableDataManagerCallback getUpsertTableDataManagerCallback() {
    try {
      return upsertTableDataManagerCallBackClass.newInstance();
    } catch (Exception ex) {
      LOGGER.error("failed to initialize new table data manager callback {}", upsertTableDataManagerCallBackClass.getName());
      ExceptionUtils.rethrow(ex);
    }
    return null;
  }

  public TableDataManagerCallback getDefaultTableDataManagerCallback() {
    try {
      return defaultTableDataManagerCallBackClass.newInstance();
    } catch (Exception ex) {
      LOGGER.error("failed to initialize new table data manager callback {}", upsertTableDataManagerCallBackClass.getName());
      ExceptionUtils.rethrow(ex);
    }
    return null;
  }
}
