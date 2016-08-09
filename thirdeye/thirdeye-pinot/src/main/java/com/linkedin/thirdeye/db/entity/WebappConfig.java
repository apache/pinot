package com.linkedin.thirdeye.db.entity;

import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Table;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigClassFactory.WebappConfigType;

@Entity
@Table(name = "webapp_configs")

public class WebappConfig  extends AbstractBaseEntity {

  @Column(name = "collection", nullable = false)
  private String collection;

  @Enumerated(EnumType.STRING)
  @Column(name = "config_type", nullable = false)
  private WebappConfigType configType;

  @Column(name = "config", nullable = false, length = 10000)
  private String config;

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public WebappConfigType getConfigType() {
    return configType;
  }

  public void setConfigType(WebappConfigType configType) {
    this.configType = configType;
  }

  public String getConfig() {
    return config;
  }

  public void setConfig(String config) {
    this.config = config;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof WebappConfig)) {
      return false;
    }
    WebappConfig wc = (WebappConfig) o;
    return Objects.equals(getId(), wc.getId())
        && Objects.equals(collection, wc.getCollection())
        && Objects.equals(configType, wc.getConfigType())
        && Objects.equals(config, wc.getConfig());
  }

  @Override public int hashCode() {
    return Objects.hash(getId(), collection, configType, config);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", getId())
        .add("collection", getCollection())
        .add("configType", getConfigType())
        .add("config", getConfigType()).toString();
  }

}
