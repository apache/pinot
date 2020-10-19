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

package org.apache.pinot.thirdeye.datalayer.bao;

import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.URL;
import java.sql.Connection;
import org.apache.commons.io.output.NullWriter;
import org.apache.pinot.thirdeye.datalayer.ScriptRunner;
import org.apache.pinot.thirdeye.datalayer.util.DaoProviderUtil;
import org.apache.pinot.thirdeye.datalayer.util.PersistenceConfig;
import org.apache.tomcat.jdbc.pool.DataSource;

public class DAOTestBase {

  //  protected TestDBResources testDBResources;
//  protected DAORegistry daoRegistry;
  DataSource ds;
  String dbUrlId;

  private DAOTestBase(){
    init();
  }

  public static DAOTestBase getInstance(){
    return new DAOTestBase();
  }

  protected void init() {
    try {
      URL url = DAOTestBase.class.getResource("/persistence-local.yml");
      File configFile = new File(url.toURI());
      PersistenceConfig configuration = DaoProviderUtil.readPersistenceConfig(configFile);
      initializeDs(configuration);

      DaoProviderUtil.init(ds);

//      daoRegistry = DAORegistry.getInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void cleanup() {
    try {
      cleanUpJDBC();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void initializeDs(PersistenceConfig configuration) throws Exception {
    ds = new DataSource();
    dbUrlId =
        configuration.getDatabaseConfiguration().getUrl() + System.currentTimeMillis() + "" + Math
            .random();
    ds.setUrl(dbUrlId);
    System.out.println("Creating db with connection url : " + ds.getUrl());
    ds.setPassword(configuration.getDatabaseConfiguration().getPassword());
    ds.setUsername(configuration.getDatabaseConfiguration().getUser());
    ds.setDriverClassName(configuration.getDatabaseConfiguration().getProperties()
        .get("hibernate.connection.driver_class"));

    // pool size configurations
    ds.setMaxActive(200);
    ds.setMinIdle(10);
    ds.setInitialSize(10);

    // when returning connection to pool
    ds.setTestOnReturn(true);
    ds.setRollbackOnReturn(true);

    // Timeout before an abandoned(in use) connection can be removed.
    ds.setRemoveAbandonedTimeout(600_000);
    ds.setRemoveAbandoned(true);

    Connection conn = ds.getConnection();
    // create schema
    URL createSchemaUrl = getClass().getResource("/schema/create-schema.sql");
    ScriptRunner scriptRunner = new ScriptRunner(conn, true);
    scriptRunner.setDelimiter(";");
    scriptRunner.setLogWriter(new PrintWriter(new NullWriter()));
    scriptRunner.runScript(new FileReader(createSchemaUrl.getFile()));
  }

  private void cleanUpJDBC() throws Exception {
    System.out.println("Cleaning database: start");
    try (Connection conn = ds.getConnection()) {
      URL deleteSchemaUrl = getClass().getResource("/schema/drop-tables.sql");
      ScriptRunner scriptRunner = new ScriptRunner(conn, false);
      scriptRunner.setLogWriter(new PrintWriter(new NullWriter()));
      scriptRunner.runScript(new FileReader(deleteSchemaUrl.getFile()));
    }
    new File(dbUrlId).delete();
    System.out.println("Cleaning database: done!");
  }
}
