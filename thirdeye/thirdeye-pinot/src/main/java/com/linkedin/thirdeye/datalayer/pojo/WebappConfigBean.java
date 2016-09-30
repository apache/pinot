package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;

/**
 * Entity class for webapp configs. name, collection, type conbination should be unique
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WebappConfigBean extends AbstractBean {
  private String name;
  private String collection;
  private WebappConfigType type;
  private Map<String, Object> configMap;

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

  public Map<String, Object> getConfigMap() {
    return configMap;
  }

  public void setConfigMap(Map<String, Object> configMap) {
    this.configMap = configMap;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof WebappConfigBean)) {
      return false;
    }
    WebappConfigBean wc = (WebappConfigBean) o;
    return Objects.equals(getId(), wc.getId()) && Objects.equals(name, wc.getName())
        && Objects.equals(collection, wc.getCollection()) && Objects.equals(type, wc.getType())
        && Objects.equals(configMap, wc.getConfigMap());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), name, collection, type, configMap);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("name", getName())
        .add("collection", getCollection()).add("type", getType()).add("config", getConfigMap())
        .toString();
  }

}
