package com.linkedin.thirdeye.datalayer.util;

import com.linkedin.thirdeye.datalayer.entity.AlertConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFunctionExIndex;
import com.linkedin.thirdeye.datalayer.entity.EventIndex;
import com.linkedin.thirdeye.datalayer.entity.AutotuneConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.OverrideConfigIndex;

import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;

import java.io.File;
import java.sql.Connection;

import javax.validation.Validation;

import org.apache.tomcat.jdbc.pool.DataSource;

import com.google.common.base.CaseFormat;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AbstractManagerImpl;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFeedbackIndex;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFunctionIndex;
import com.linkedin.thirdeye.datalayer.entity.DashboardConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.DataCompletenessConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.DatasetConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.DetectionStatusIndex;
import com.linkedin.thirdeye.datalayer.entity.EmailConfigurationIndex;
import com.linkedin.thirdeye.datalayer.entity.GenericJsonEntity;
import com.linkedin.thirdeye.datalayer.entity.IngraphDashboardConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.IngraphMetricConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.JobIndex;
import com.linkedin.thirdeye.datalayer.entity.MergedAnomalyResultIndex;
import com.linkedin.thirdeye.datalayer.entity.MetricConfigIndex;
import com.linkedin.thirdeye.datalayer.entity.RawAnomalyResultIndex;
import com.linkedin.thirdeye.datalayer.entity.TaskIndex;

public abstract class DaoProviderUtil {

  private static DataSource dataSource;
  private static ManagerProvider provider;

  public static void init(File localConfigFile) {
    PersistenceConfig configuration = createConfiguration(localConfigFile);
    dataSource = new DataSource();
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
    init(dataSource);
  }

  public static void init (DataSource ds) {
    dataSource = ds;
    provider = new ManagerProvider(dataSource);
  }

  public static PersistenceConfig createConfiguration(File configFile) {
    ConfigurationFactory<PersistenceConfig> factory =
        new ConfigurationFactory<>(PersistenceConfig.class,
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
    T instance = provider.getInstance(c);
    return instance;
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
        entityMappingHolder.register(conn, DatasetConfigIndex.class,
            convertCamelCaseToUnderscore(DatasetConfigIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, MetricConfigIndex.class,
            convertCamelCaseToUnderscore(MetricConfigIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, DashboardConfigIndex.class,
            convertCamelCaseToUnderscore(DashboardConfigIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, IngraphDashboardConfigIndex.class,
            convertCamelCaseToUnderscore(IngraphDashboardConfigIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, IngraphMetricConfigIndex.class,
            convertCamelCaseToUnderscore(IngraphMetricConfigIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, OverrideConfigIndex.class,
            convertCamelCaseToUnderscore(OverrideConfigIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, AlertConfigIndex.class,
            convertCamelCaseToUnderscore(AlertConfigIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, DataCompletenessConfigIndex.class,
            convertCamelCaseToUnderscore(DataCompletenessConfigIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, EventIndex.class,
            convertCamelCaseToUnderscore(EventIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, DetectionStatusIndex.class,
            convertCamelCaseToUnderscore(DetectionStatusIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, AutotuneConfigIndex.class,
            convertCamelCaseToUnderscore(AutotuneConfigIndex.class.getSimpleName()));
        entityMappingHolder.register(conn, AnomalyFunctionExIndex.class,
            convertCamelCaseToUnderscore(AnomalyFunctionExIndex.class.getSimpleName()));
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

    @Provides
    GenericResultSetMapper getResultSetMapper() {
      return genericResultSetMapper;
    }
  }
}
