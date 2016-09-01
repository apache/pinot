package com.linkedin.thirdeye.datalayer.util;

import com.google.common.base.CaseFormat;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.linkedin.thirdeye.common.persistence.PersistenceConfig;
import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFeedback;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFunction;
import com.linkedin.thirdeye.datalayer.entity.AnomalyMergedResult;
import com.linkedin.thirdeye.datalayer.entity.AnomalyRawResult;
import com.linkedin.thirdeye.datalayer.entity.EmailConfiguration;
import com.linkedin.thirdeye.datalayer.entity.Job;
import com.linkedin.thirdeye.datalayer.entity.Task;
import com.linkedin.thirdeye.datalayer.entity.WebappConfig;

import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import java.io.File;
import java.sql.Connection;
import javax.validation.Validation;
import org.apache.tomcat.jdbc.pool.DataSource;

public abstract class DaoProviderUtil {

  private static Injector injector;

  public static void init(File localConfigFile) {
    PersistenceConfig configuration = PersistenceUtil.createConfiguration(localConfigFile);
    DataSource dataSource = new DataSource();
    dataSource.setInitialSize(10);
    dataSource.setDefaultAutoCommit(true);
    dataSource.setMaxActive(100);
    dataSource.setUsername(configuration.getDatabaseConfiguration().getUser());
    dataSource.setPassword(configuration.getDatabaseConfiguration().getPassword());
    dataSource.setUrl(configuration.getDatabaseConfiguration().getUrl());
    dataSource.setDriverClassName(configuration.getDatabaseConfiguration().getDriver());

    DataSourceModule dataSourceModule = new DataSourceModule(dataSource);
    injector = Guice.createInjector(dataSourceModule);
  }

  public static PersistenceConfig createConfiguration(File configFile) {
    ConfigurationFactory<PersistenceConfig> factory = new ConfigurationFactory<>(
        PersistenceConfig.class, Validation.buildDefaultValidatorFactory().getValidator(),
        Jackson.newObjectMapper(), "");
    PersistenceConfig configuration;
    try {
      configuration = factory.build(configFile);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return configuration;
  }

  public static <T> T getInstance(Class<T> c) {
    return injector.getInstance(c);
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
        entityMappingHolder.register(conn, AnomalyFeedback.class, convertCamelCaseToUnderscore(AnomalyFeedback.class.getName()));
        entityMappingHolder.register(conn, AnomalyFunction.class, convertCamelCaseToUnderscore(AnomalyFunction.class.getName()));
        entityMappingHolder.register(conn, Job.class, convertCamelCaseToUnderscore(Job.class.getName()));
        entityMappingHolder.register(conn, AnomalyMergedResult.class, convertCamelCaseToUnderscore(AnomalyMergedResult.class.getName()));
        entityMappingHolder.register(conn, AnomalyRawResult.class, convertCamelCaseToUnderscore(AnomalyRawResult.class.getName()));
        entityMappingHolder.register(conn, Task.class, convertCamelCaseToUnderscore(Task.class.getName()));
        entityMappingHolder.register(conn, EmailConfiguration.class, convertCamelCaseToUnderscore(EmailConfiguration.class.getName()));
        entityMappingHolder.register(conn, WebappConfig.class, convertCamelCaseToUnderscore(WebappConfig.class.getName()));
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
    protected void configure() {
    }

    @Provides
    javax.sql.DataSource getDataSource() {
      return dataSource;
    }

    @Provides
    SqlQueryBuilder getBuilder() {
      return builder;
    }

    @Provides GenericResultSetMapper getResultSetMapper() {
      return genericResultSetMapper;
    }
  }
}
