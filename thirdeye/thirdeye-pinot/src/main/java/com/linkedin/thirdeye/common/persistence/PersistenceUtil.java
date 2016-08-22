package com.linkedin.thirdeye.common.persistence;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.persist.PersistService;
import com.google.inject.persist.jpa.JpaPersistModule;

import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;

import java.io.File;
import java.util.Map.Entry;
import java.util.Properties;

import javax.validation.Validation;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.jdbc.connections.internal.DatasourceConnectionProviderImpl;

public abstract class PersistenceUtil {

  public static final String JPA_UNIT = "te";
  private static Injector injector;

  private PersistenceUtil() {
  }

  // Used for unit testing, provides injector
  public static void init(File localConfigFile) {
    PersistenceConfig configuration = createConfiguration(localConfigFile);
    Properties properties = createDbPropertiesFromConfiguration(configuration);

    DataSource ds = new DataSource();
    ds.setUrl(configuration.getDatabaseConfiguration().getUrl());
    ds.setPassword(configuration.getDatabaseConfiguration().getPassword());
    ds.setUsername(configuration.getDatabaseConfiguration().getUser());
    ds.setDriverClassName(configuration.getDatabaseConfiguration().getProperties().get("hibernate.connection.driver_class"));
    ds.setMaxActive(200);
    ds.setInitialSize(10);
    ds.setDefaultAutoCommit(true);

    properties.put(Environment.CONNECTION_PROVIDER, DatasourceConnectionProviderImpl.class.getName());
    properties.put(Environment.DATASOURCE, ds);

    JpaPersistModule jpaPersistModule = new JpaPersistModule(JPA_UNIT).properties(properties);
    injector = Guice.createInjector(jpaPersistModule, new PersistenceModule());
    injector.getInstance(PersistService.class).start();
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

  public static Properties createDbPropertiesFromConfiguration(PersistenceConfig localConfiguration) {
    PersistenceConfig.DatabaseConfiguration databaseConfiguration = localConfiguration.getDatabaseConfiguration();
    Properties properties = new Properties();
    for (Entry<String, String> entry : databaseConfiguration.getProperties().entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }
    return properties;
  }

  public static Injector getInjector() {
    if (injector == null) {
      throw new RuntimeException("call init() first!");
    }
    return injector;
  }

  public static <T> T getInstance(Class<T> c) {
    return getInjector().getInstance(c);
  }
}
