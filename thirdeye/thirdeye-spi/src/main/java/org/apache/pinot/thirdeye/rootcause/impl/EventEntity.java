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

import org.apache.pinot.thirdeye.rootcause.Entity;
import java.util.ArrayList;
import java.util.List;


/**
 * EventEntity represents an individual event. It holds meta-data referencing ThirdEye's internal
 * database. The URN namespace is defined as 'thirdeye:event:{type}:{id}'.
 */
public class EventEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:event:");

  private final String eventType;
  private final long id;

  protected EventEntity(String urn, double score, List<? extends Entity> related, String eventType, long id) {
    super(urn, score, related);
    this.id = id;
    this.eventType = eventType;
  }

  public String getEventType() {
    return eventType;
  }

  public long getId() {
    return id;
  }

  @Override
  public EventEntity withScore(double score) {
    return new EventEntity(this.getUrn(), score, this.getRelated(), this.eventType, this.id);
  }

  @Override
  public EventEntity withRelated(List<? extends Entity> related) {
    return new EventEntity(this.getUrn(), this.getScore(), related, this.eventType, this.id);
  }

  public static EventEntity fromURN(String urn, double score) {
    if(!TYPE.isType(urn))
      throw new IllegalArgumentException(String.format("URN '%s' is not type '%s'", urn, TYPE.getPrefix()));
    String[] parts = urn.split(":", 4);
    if(parts.length != 4)
      throw new IllegalArgumentException(String.format("URN must have 3 parts but has '%d'", parts.length));
    return new EventEntity(urn, score, new ArrayList<Entity>(), parts[2], Long.parseLong(parts[3]));
  }
}
