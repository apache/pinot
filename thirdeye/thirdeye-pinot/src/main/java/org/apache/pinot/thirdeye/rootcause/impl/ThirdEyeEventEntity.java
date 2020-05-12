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

import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.rootcause.Entity;

import java.util.ArrayList;
import java.util.List;


public class ThirdEyeEventEntity extends EventEntity {

  private final EventDTO dto;

  private ThirdEyeEventEntity(String urn, double score, List<? extends Entity> related, long id, EventDTO dto, String eventType) {
    super(urn, score, related, eventType, id);
    this.dto = dto;
  }

  public EventDTO getDto() {
    return dto;
  }

  @Override
  public ThirdEyeEventEntity withScore(double score) {
    return new ThirdEyeEventEntity(this.getUrn(), score, this.getRelated(), this.getId(), this.dto, getEventType());
  }

  @Override
  public ThirdEyeEventEntity withRelated(List<? extends Entity> related) {
    return new ThirdEyeEventEntity(this.getUrn(), this.getScore(), related, this.getId(), this.dto, getEventType());
  }

  public static ThirdEyeEventEntity fromDTO(double score, EventDTO dto, String eventType) {
    EntityType type = new EntityType(EventEntity.TYPE.getPrefix() + eventType + ":");
    String urn = type.formatURN(dto.getId());
    return new ThirdEyeEventEntity(urn, score, new ArrayList<Entity>(), dto.getId(), dto, eventType);
  }

  public static ThirdEyeEventEntity fromDTO(double score, List<? extends Entity> related, EventDTO dto, String eventType) {
    EntityType type = new EntityType(EventEntity.TYPE.getPrefix() + eventType + ":");
    String urn = type.formatURN(dto.getId());
    return new ThirdEyeEventEntity(urn, score, related, dto.getId(), dto, eventType);
  }
}
