/*
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

package org.apache.pinot.thirdeye.datalayer.util;

import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import javax.validation.Validation;
import org.apache.pinot.thirdeye.datalayer.ScriptRunner;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.AbstractManagerImpl;
import org.apache.pinot.thirdeye.datalayer.dto.AbstractDTO;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.h2.store.fs.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DaoProviderUtil {

  private static final Logger LOG = LoggerFactory.getLogger(DaoProviderUtil.class);

  private static final String DEFAULT_DATABASE_PATH = "jdbc:h2:./config/h2db";
  private static final String DEFAULT_DATABASE_FILE = "./config/h2db.mv.db";

  private static DataSource dataSource;
  private static ManagerProvider provider;

  public static void init(File localConfigFile) {
    final PersistenceConfig configuration = createConfiguration(localConfigFile);
    final DataSource dataSource = createDataSource(configuration);

    // create schema for default database
    createSchemaIfReqd(configuration);

    init(dataSource);
  }

  private static void createSchemaIfReqd(final PersistenceConfig configuration) {
    if (configuration.getDatabaseConfiguration().getUrl().equals(DEFAULT_DATABASE_PATH)
        && !FileUtils.exists(DEFAULT_DATABASE_FILE)) {
      try {
        LOG.info("Creating database schema for default URL '{}'", DEFAULT_DATABASE_PATH);
        Connection conn = dataSource.getConnection();
        final ScriptRunner scriptRunner = new ScriptRunner(conn, false);
        scriptRunner.setDelimiter(";");

        InputStream createSchema = DaoProviderUtil.class
            .getResourceAsStream("/schema/create-schema.sql");
        scriptRunner.runScript(new InputStreamReader(createSchema));
      } catch (Exception e) {
        LOG.error("Could not create database schema. Attempting to use existing.", e);
      }
    } else {
      LOG.info("Using existing database at '{}'",
          configuration.getDatabaseConfiguration().getUrl());
    }
  }

  private static DataSource createDataSource(final PersistenceConfig configuration) {
    final DataSource dataSource = new DataSource();
    dataSource.setInitialSize(10);
    dataSource.setDefaultAutoCommit(false);
    dataSource.setMaxActive(100);
    dataSource.setUsername(configuration.getDatabaseConfiguration().getUser());
    dataSource.setPassword(configuration.getDatabaseConfiguration().getPassword());
    dataSource.setUrl(configuration.getDatabaseConfiguration().getUrl());
    dataSource.setDriverClassName(configuration.getDatabaseConfiguration().getDriver());

    dataSource.setValidationQuery("select 1");
    dataSource.setTestWhileIdle(true);
    dataSource.setTestOnBorrow(true);
    // when returning connection to pool
    dataSource.setTestOnReturn(true);
    dataSource.setRollbackOnReturn(true);

    // Timeout before an abandoned(in use) connection can be removed.
    dataSource.setRemoveAbandonedTimeout(600_000);
    dataSource.setRemoveAbandoned(true);
    return dataSource;
  }

  public static void init(DataSource ds) {
    dataSource = ds;
    provider = new ManagerProvider(dataSource);
  }

  public static PersistenceConfig createConfiguration(File configFile) {
    YamlConfigurationFactory<PersistenceConfig> factory =
        new YamlConfigurationFactory<>(PersistenceConfig.class,
            Validation.buildDefaultValidatorFactory().getValidator(), Jackson.newObjectMapper(),
            "");
    PersistenceConfig configuration;
    try {
      configuration = factory.build(configFile);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return configuration;
  }

  public static <T extends AbstractManagerImpl<? extends AbstractDTO>> T getInstance(Class<T> c) {
    return provider.getInstance(c);
  }
}
