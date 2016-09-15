package com.linkedin.thirdeye.datalayer.entity;

public class WebappConfigIndex extends AbstractIndexEntity {
  String name;
  String collection;
  String type;

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  @Override
  public String toString() {
    return "WebappConfigIndex [name=" + name + ", collection=" + collection + ", type=" + type
        + ", baseId=" + baseId + ", id=" + id + ", createTime=" + createTime + ", updateTime="
        + updateTime + ", version=" + version + "]";
  }
  
  
}
