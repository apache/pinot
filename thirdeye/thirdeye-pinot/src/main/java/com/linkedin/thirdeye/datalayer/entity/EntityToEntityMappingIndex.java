package com.linkedin.thirdeye.datalayer.entity;

import com.linkedin.thirdeye.datalayer.pojo.EntityToEntityMappingBean.MappingType;

public class EntityToEntityMappingIndex extends AbstractIndexEntity {
  String fromUrn;
  String toUrn;
  MappingType mappingType;

  public String getFromUrn() {
    return fromUrn;
  }
  public void setFromUrn(String fromUrn) {
    this.fromUrn = fromUrn;
  }
  public String getToUrn() {
    return toUrn;
  }
  public void setToUrn(String toUrn) {
    this.toUrn = toUrn;
  }
  public MappingType getMappingType() {
    return mappingType;
  }
  public void setMappingType(MappingType mappingType) {
    this.mappingType = mappingType;
  }


}
