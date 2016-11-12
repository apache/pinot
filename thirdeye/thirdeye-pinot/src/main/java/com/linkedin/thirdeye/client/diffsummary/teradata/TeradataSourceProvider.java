package com.linkedin.thirdeye.client.diffsummary.teradata;

import com.google.inject.Provider;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.datalayer.util.PersistenceConfig;
import java.io.File;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TeradataSourceProvider implements Provider<DataSource> {
  protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

  private DataSource dataSource;

  public TeradataSourceProvider () {
    //TODO: change this to actual persistence-tera directory
//    String persistenceConfig = System.getProperty("dw.rootDir") + "/persistence-tera.yml";
    String persistenceConfig = "/Users/jswang/tmp/thirdeye-configs/persistence-tera.yml";
    LOG.info("Loading Teradata persistence config from [{}]", persistenceConfig);
    File teraConfigFile = new File(persistenceConfig);
    PersistenceConfig configuration = DaoProviderUtil.createConfiguration(teraConfigFile);
    this.dataSource = new DataSource();
    this.dataSource.setInitialSize(5);
    this.dataSource.setDefaultAutoCommit(false);
    this.dataSource.setMaxActive(100);
    this.dataSource.setUsername(configuration.getDatabaseConfiguration().getUser());
    this.dataSource.setPassword(configuration.getDatabaseConfiguration().getPassword());
    this.dataSource.setUrl(configuration.getDatabaseConfiguration().getUrl());
    this.dataSource.setDriverClassName(configuration.getDatabaseConfiguration().getDriver());

    this.dataSource.setValidationQuery("select 1");
    this.dataSource.setTestWhileIdle(true);
    this.dataSource.setTestOnBorrow(true);
    this.dataSource.setTestOnReturn(true);
    this.dataSource.setRollbackOnReturn(true);

    this.dataSource.setRemoveAbandonedTimeout(600_000);
    this.dataSource.setRemoveAbandoned(true);
  }

  @Override
  public DataSource get() {
    return dataSource;
  }
}
