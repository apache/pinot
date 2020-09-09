/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.pinot.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Objects;

/**
 * This class holds the mapping between entities
 * We have predefined some valid relations
 * Each mapping can maintain a score, to say whay it the degree of correlation between the entities
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EntityToEntityMappingBean extends AbstractBean {
  String fromURN;
  String toURN;
  String mappingType;
  double score;

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
    return Objects.equals(fromURN, em.getFromURN())
        && Objects.equals(toURN, em.getToURN())
        && Objects.equals(mappingType, em.getMappingType())
        && Objects.equals(score, em.getScore());
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromURN, toURN, mappingType, score);
  }

}
