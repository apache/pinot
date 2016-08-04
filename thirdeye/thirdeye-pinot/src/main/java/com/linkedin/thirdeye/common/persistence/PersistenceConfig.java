package com.linkedin.thirdeye.common.persistence;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.dropwizard.Configuration;
import java.util.Map;

public class PersistenceConfig extends Configuration {

  /**
   * Persistence specific file will be in
   * <configRootDir>/persistence.yml
   */
  private DatabaseConfiguration databaseConfiguration;

  @JsonProperty
  public DatabaseConfiguration getDatabaseConfiguration() {
    return databaseConfiguration;
  }

  public void setDatabaseConfiguration(DatabaseConfiguration databaseConfiguration) {
    this.databaseConfiguration = databaseConfiguration;
  }

  public static class DatabaseConfiguration {
    private String user;
    private String password;
    private String url;
    private Map<String, String> properties = Maps.newLinkedHashMap();

    public String getUser() {
      return user;
    }

    public void setUser(String user) {
      this.user = user;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public Map<String, String> getProperties() {
      return properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }
  }
}
