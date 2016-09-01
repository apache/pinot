package com.linkedin.thirdeye.datalayer.dto;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;
import com.linkedin.thirdeye.db.entity.AbstractBaseEntity;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

/**
 * Entity class for webapp configs. name, collection, type conbination should be unique
 */
public class IngraphMetricConfigDTO extends AbstractDTO {

  private String name;

  private String collection;

  private WebappConfigType type;

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
    if (!(o instanceof IngraphMetricConfigDTO)) {
      return false;
    }
    IngraphMetricConfigDTO wc = (IngraphMetricConfigDTO) o;
    return Objects.equals(getId(), wc.getId())
        && Objects.equals(name, wc.getName())
        && Objects.equals(collection, wc.getCollection())
        && Objects.equals(type, wc.getType())
        && Objects.equals(config, wc.getConfig());
  }

  @Override public int hashCode() {
    return Objects.hash(getId(), name, collection, type, config);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", getId())
        .add("name", getName())
        .add("collection", getCollection())
        .add("type", getType())
        .add("config", getConfig()).toString();
  }

}
