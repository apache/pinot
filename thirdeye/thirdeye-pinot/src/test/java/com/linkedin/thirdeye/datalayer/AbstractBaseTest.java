package com.linkedin.thirdeye.datalayer;

import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;

import javax.sql.DataSource;

import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

public class AbstractBaseTest {
  ScriptRunner scriptRunner;
  Connection conn;

  public void init() throws Exception {
    URL configUrl = getClass().getResource("/persistence-local.yml");
    File configFile = new File(configUrl.toURI());
    DaoProviderUtil.init(configFile);
    DataSource ds = DaoProviderUtil.getInstance(DataSource.class);
    conn = ds.getConnection();

    // create schema
    URL createSchemaUrl = getClass().getResource("/schema/create-schema.sql");
    scriptRunner = new ScriptRunner(conn, false, false);
    scriptRunner.setDelimiter(";", true);
    scriptRunner.runScript(new FileReader(createSchemaUrl.getFile()));
  }

  public void cleanUp() throws Exception {
    URL deleteSchemaUrl = getClass().getResource("/schema/drop-tables.sql");
    scriptRunner.runScript(new FileReader(deleteSchemaUrl.getFile()));
  }

  public static void main(String[] args) throws Exception {
    AbstractBaseTest baseTest = new AbstractBaseTest();
    try {
      baseTest.init();
    } finally {
      // this drops the tables
      baseTest.cleanUp();
    }
  }
}
