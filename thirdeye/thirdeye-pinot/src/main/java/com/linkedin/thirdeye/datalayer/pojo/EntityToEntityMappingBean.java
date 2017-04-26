package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Objects;

/**
 * This class holds the mapping between entities
 * We have predefined some valid relations
 * Each mapping can maintain a score, to say whay it the degree of correlation between the entities
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EntityToEntityMappingBean extends AbstractBean {

  public enum MappingType {
    METRIC_TO_SERVICE,
    METRIC_TO_METRIC,
    DIMENSION_TO_DIMENSION
  }
  String fromUrn;
  String toUrn;
  MappingType mappingType;
  double score;

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
  public double getScore() {
    return score;
  }
  public void setScore(double score) {
    this.score = score;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof EntityToEntityMappingBean)) {
      return false;
    }
    EntityToEntityMappingBean em = (EntityToEntityMappingBean) o;
    return Objects.equals(fromUrn, em.getFromUrn())
        && Objects.equals(toUrn, em.getToUrn())
        && Objects.equals(mappingType, em.getMappingType())
        && Objects.equals(score, em.getScore());
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromUrn, toUrn, mappingType, score);
  }

}
