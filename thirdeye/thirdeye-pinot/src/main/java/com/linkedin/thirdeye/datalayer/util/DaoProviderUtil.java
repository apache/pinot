package com.linkedin.thirdeye.datalayer.util;

import java.io.File;
import java.sql.Connection;

import org.apache.tomcat.jdbc.pool.DataSource;

import com.google.common.base.CaseFormat;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.linkedin.thirdeye.common.persistence.PersistenceConfig;
import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFeedbackIndex;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFunctionIndex;
import com.linkedin.thirdeye.datalayer.entity.EmailConfigurationIndex;
import com.linkedin.thirdeye.datalayer.entity.GenericJsonEntity;
import com.linkedin.thirdeye.datalayer.entity.JobIndex;
import com.linkedin.thirdeye.datalayer.entity.MergedAnomalyResultIndex;
import com.linkedin.thirdeye.datalayer.entity.RawAnomalyResultIndex;
import com.linkedin.thirdeye.datalayer.entity.TaskIndex;
import com.linkedin.thirdeye.datalayer.entity.WebappConfigIndex;

public abstract class DaoProviderUtil {

  private static Injector injector;
  private static DataSource dataSource;
  static boolean inited = false;

  public static void init(File localConfigFile) {
    PersistenceConfig configuration = PersistenceUtil.createConfiguration(localConfigFile);
    dataSource = new DataSource();
    dataSource.setInitialSize(10);
    dataSource.setDefaultAutoCommit(true);
    dataSource.setMaxActive(100);
    dataSource.setUsername(configuration.getDatabaseConfiguration().getUser());
    dataSource.setPassword(configuration.getDatabaseConfiguration().getPassword());
    dataSource.setUrl(configuration.getDatabaseConfiguration().getUrl());
    dataSource.setDriverClassName(configuration.getDatabaseConfiguration().getDriver());
  }

  public static void init(DataSource ds) {
    dataSource = ds;
  }

  private synchronized static void initGuice() {
    if (!inited) {
      DataSourceModule dataSourceModule = new DataSourceModule(dataSource);
      injector = Guice.createInjector(dataSourceModule);
      inited = true;
    }
  }

  public static <T> T getInstance(Class<T> c) {
    if (!inited) {
      synchronized (DaoProviderUtil.class) {
        if (!inited) {
          initGuice();
        }
      }
    }
    return injector.getInstance(c);
  }

  public static DataSource getDataSource() {
    return dataSource;
  }

  static class DataSourceModule extends AbstractModule {
    SqlQueryBuilder builder;
    DataSource dataSource;
    private GenericResultSetMapper genericResultSetMapper;
    EntityMappingHolder entityMappingHolder;

    DataSourceModule(DataSource dataSource) {
      this.dataSource = dataSource;
      entityMappingHolder = new EntityMappingHolder();
      try (Connection conn = dataSource.getConnection()) {
        entityMappingHolder.register(conn, GenericJsonEntity.class,
            convertCamelCaseToUnderscore(GenericJsonEntity.class.getSimpleName()));
        entityMappingHolder.register(conn, AnomalyFeedbackIndex.class,
            convertCamelCaseToUnderscore(AnomalyFeedbackIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, AnomalyFunctionIndex.class,
            convertCamelCaseToUnderscore(AnomalyFunctionIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, JobIndex.class,
            convertCamelCaseToUnderscore(JobIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, MergedAnomalyResultIndex.class,
            convertCamelCaseToUnderscore(MergedAnomalyResultIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, RawAnomalyResultIndex.class,
            convertCamelCaseToUnderscore(RawAnomalyResultIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, TaskIndex.class,
            convertCamelCaseToUnderscore(TaskIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, EmailConfigurationIndex.class,
            convertCamelCaseToUnderscore(EmailConfigurationIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, WebappConfigIndex.class,
            convertCamelCaseToUnderscore(WebappConfigIndex.class.getSimpleName()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      builder = new SqlQueryBuilder(entityMappingHolder);

      genericResultSetMapper = new GenericResultSetMapper(entityMappingHolder);
    }

    public static String convertCamelCaseToUnderscore(String str) {
      return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, str);
    }

    @Override
    protected void configure() {}

    @Provides
    javax.sql.DataSource getDataSource() {
      return dataSource;
    }

    @Provides
    SqlQueryBuilder getBuilder() {
      return builder;
    }

    @Provides
    GenericResultSetMapper getResultSetMapper() {
      return genericResultSetMapper;
    }
  }
}
