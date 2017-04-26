package com.linkedin.thirdeye.datalayer.entity;

import com.linkedin.thirdeye.datalayer.pojo.EntityToEntityMappingBean.MappingType;

public class EntityToEntityMappingIndex extends AbstractIndexEntity {
  String fromURN;
  String toURN;
  MappingType mappingType;

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
  public MappingType getMappingType() {
    return mappingType;
  }
  public void setMappingType(MappingType mappingType) {
    this.mappingType = mappingType;
  }


}
