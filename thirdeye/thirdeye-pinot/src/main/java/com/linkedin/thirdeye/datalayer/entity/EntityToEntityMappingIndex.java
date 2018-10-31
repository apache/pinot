package com.linkedin.thirdeye.datalayer.entity;

public class EntityToEntityMappingIndex extends AbstractIndexEntity {
  String fromURN;
  String toURN;
  String mappingType;

  public String getFromURN() {
    return fromURN;
  }
  public void setFromURN(String fromURN) {
    this.fromURN = fromURN;
  }
  public String getToURN() {
    return toURN;
  }
  public void setToURN(String toURN) {
    this.toURN = toURN;
  }
  public String getMappingType() {
    return mappingType;
  }
  public void setMappingType(String mappingType) {
    this.mappingType = mappingType;
  }


}
