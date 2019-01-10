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
 */

package org.apache.pinot.thirdeye.rootcause.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.rootcause.Entity;
import org.apache.pinot.thirdeye.rootcause.PipelineContext;
import org.apache.pinot.thirdeye.rootcause.util.EntityUtils;
import org.apache.pinot.thirdeye.rootcause.util.ParsedUrn;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * DimensionEntity represents a data dimension (a cut) across multiple metrics. It is identified
 * by a key-value pair. Note, that dimension names may require standardization across different
 * metrics. The URN namespace is defined as 'thirdeye:dimension:{name}:{value}:{type}'.
 */
@Deprecated
public class DimensionEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:dimension:");

  public static final String TYPE_PROVIDED = "provided"; // user-defined filter to be applied everywhere
  public static final String TYPE_GENERATED = "generated"; // pipeline-generated cut of data

  private final String name;
  private final String value;
  private final String type;

  protected DimensionEntity(String urn, double score, List<? extends Entity> related, String name, String value, String type) {
    super(urn, score, related);
    this.name = name;
    this.value = value;
    this.type = type;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  @Override
  public DimensionEntity withScore(double score) {
    return new DimensionEntity(this.getUrn(), score, this.getRelated(), this.name, this.value, this.type);
  }

  public static DimensionEntity fromDimension(double score, Collection<? extends Entity> related, String name, String value, String type) {
    return new DimensionEntity(TYPE.formatURN(EntityUtils.encodeURNComponent(name), EntityUtils.encodeURNComponent(value), type), score, new ArrayList<>(related), name, value, type);
  }

  public static DimensionEntity fromDimension(double score, String name, String value, String type) {
    return fromDimension(score, new ArrayList<Entity>(), name, value, type);
  }

  @Override
  public DimensionEntity withRelated(List<? extends Entity> related) {
    return new DimensionEntity(this.getUrn(), this.getScore(), related, this.name, this.value, this.type);
  }

  public static DimensionEntity fromURN(String urn, double score) {
    if(!TYPE.isType(urn))
      throw new IllegalArgumentException(String.format("URN '%s' is not type '%s'", urn, TYPE.getPrefix()));
    String[] parts = urn.split(":", 5);
    if(parts.length != 5)
      throw new IllegalArgumentException(String.format("Dimension URN must have 5 parts but has '%d'", parts.length));
    return fromDimension(score, EntityUtils.decodeURNComponent(parts[2]), EntityUtils.decodeURNComponent(parts[3]), parts[4]);

  }

  public static  Set<DimensionEntity> getContextDimensions(PipelineContext context, String type) {
    Set<DimensionEntity> output = new HashSet<>();
    for (DimensionEntity e : context.filter(DimensionEntity.class)) {
      if (type.equals(e.type)) {
        output.add(e);
      }
    }
    return output;
  }

  public static Set<DimensionEntity> getContextDimensionsProvided(PipelineContext context) {
    return getContextDimensions(context, TYPE_PROVIDED);
  }

  public static Set<DimensionEntity> getContextDimensionsGenerated(PipelineContext context) {
    return getContextDimensions(context, TYPE_GENERATED);
  }

  public static Multimap<String, String> makeFilterSet(PipelineContext context) {
    return makeFilterSet(getContextDimensionsProvided(context));
  }

  public static Multimap<String, String> makeFilterSet(Set<DimensionEntity> entities) {
    Multimap<String, String> filters = ArrayListMultimap.create();
    for (DimensionEntity e : entities) {
      if (TYPE_PROVIDED.equals(e.getType())) {
        filters.put(e.name, e.value);
      }
    }
    return filters;
  }
}
