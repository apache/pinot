/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.service;

import java.io.File;
import java.io.IOException;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.services.ServiceStartable;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AddTableStarter implements ServiceStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddTableStarter.class);

  private final PinotConfiguration _config;
  private final String _instanceId;
  private final Schema _schema;
  private final TableConfig _tableConfig;
  private final String _tableNameWithType;

  PinotHelixResourceManager _helixResourceManager; // TODO: how to get this?

  public AddTableStarter(PinotConfiguration config) throws Exception {
    _config = config;

    _schema = readJsonFromFileProperty(config, "addTable.schemaFile", Schema.class);
    _tableConfig = readJsonFromFileProperty(config, "addTable.tableConfigFile", TableConfig.class);
    _tableNameWithType = TableNameBuilder
        .forType(_tableConfig.getTableType())
        .tableNameWithType(_tableConfig.getTableName());

    _instanceId = config.getProperty(CommonConstants.Helix.Instance.INSTANCE_ID_KEY,
        CommonConstants.Helix.PREFIX_OF_ADD_TABLE_INSTANCE + NetUtil.getHostAddress() + "_"
            + _tableNameWithType);
  }

  @Override
  public ServiceRole getServiceRole() {
    return ServiceRole.ADD_TABLE;
  }

  @Override
  public String getInstanceId() {
    return _instanceId;
  }

  @Override
  public PinotConfiguration getConfig() {
    return _config;
  }

  @Override
  public void start() throws IOException {
    LOGGER.info("Starting to add schema: {}", _instanceId);
    // Adding schema is synchronous, so we don't need a wait loop
    _helixResourceManager.addSchema(_schema, true);
    LOGGER.info("Starting to add table config: {}", _instanceId);
    _helixResourceManager.addTable(_tableConfig);

    // Initialize health check callback
    LOGGER.info("Initializing health check callback");
    ServiceStatus.setServiceStatusCallback(_instanceId, new ServiceStatus.ServiceStatusCallback() {
      @Override
      public ServiceStatus.Status getServiceStatus() {
        if (_helixResourceManager.hasTable(_tableNameWithType)) return ServiceStatus.Status.GOOD;
        return ServiceStatus.Status.STARTING;
      }

      @Override
      public String getStatusDescription() {
        return ServiceStatus.STATUS_DESCRIPTION_NONE;
      }
    });

    LOGGER.info("Pinot table added: {}", _instanceId);
  }

  @Override
  public void stop() {
    // nothing to do really
    ServiceStatus.removeServiceStatusCallback(_instanceId);
  }

  static <T> T readJsonFromFileProperty(PinotConfiguration config, String configName,
      Class<T> valueType) throws IOException {
    String path = config.getProperty(configName);
    if (path == null) throw new NullPointerException(configName + " config missing");
    File file = new File(path);
    if (!file.exists()) throw new NullPointerException(configName + " " + path + " does not exist");
    return JsonUtils.fileToObject(file, valueType);
  }
}
