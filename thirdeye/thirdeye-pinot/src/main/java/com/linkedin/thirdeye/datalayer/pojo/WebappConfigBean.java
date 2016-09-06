package com.linkedin.thirdeye.datalayer.pojo;

import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;

/**
 * Entity class for webapp configs. name, collection, type conbination should be unique
 */
@Entity
@Table(name = "webapp_config",
    uniqueConstraints = {@UniqueConstraint(columnNames = {"name", "collection", "type"})})
public class WebappConfigBean extends AbstractBean {

  @Column(name = "name", nullable = false)
  private String name;

  @Column(name = "collection", nullable = false)
  private String collection;

  @Enumerated(EnumType.STRING)
  @Column(name = "type", nullable = false)
  private WebappConfigType type;

  @Column(name = "config", nullable = false, length = 10000)
  private String config;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public WebappConfigType getType() {
    return type;
  }

  public void setType(WebappConfigType type) {
    this.type = type;
  }

  public String getConfig() {
    return config;
  }

  public void setConfig(String config) {
    this.config = config;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof WebappConfigBean)) {
      return false;
    }
    WebappConfigBean wc = (WebappConfigBean) o;
    return Objects.equals(getId(), wc.getId()) && Objects.equals(name, wc.getName())
        && Objects.equals(collection, wc.getCollection()) && Objects.equals(type, wc.getType())
        && Objects.equals(config, wc.getConfig());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), name, collection, type, config);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("name", getName())
        .add("collection", getCollection()).add("type", getType()).add("config", getConfig())
        .toString();
  }

}
