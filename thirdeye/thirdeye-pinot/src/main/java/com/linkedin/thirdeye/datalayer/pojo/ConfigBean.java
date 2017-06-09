package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;


@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigBean extends AbstractBean {
  private String name;
  private String namespace;
  private Object value;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfigBean that = (ConfigBean) o;
    return Objects.equals(getId(), that.getId()) &&
           Objects.equals(name, that.name) &&
           Objects.equals(namespace, that.namespace) &&
           Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), name, namespace, value);
  }
}
