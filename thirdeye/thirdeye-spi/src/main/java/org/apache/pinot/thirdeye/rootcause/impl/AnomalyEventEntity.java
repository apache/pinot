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

import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.rootcause.Entity;
import org.apache.pinot.thirdeye.rootcause.util.EntityUtils;
import org.apache.pinot.thirdeye.rootcause.util.ParsedUrn;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class AnomalyEventEntity extends EventEntity {
  public static final String EVENT_TYPE_ANOMALY = "anomaly";

  public static final EntityType TYPE = new EntityType(EventEntity.TYPE.getPrefix() + EVENT_TYPE_ANOMALY + ":");

  protected AnomalyEventEntity(String urn, double score, List<? extends Entity> related, long id) {
    super(urn, score, related, EVENT_TYPE_ANOMALY, id);
  }

  @Override
  public AnomalyEventEntity withScore(double score) {
    return new AnomalyEventEntity(this.getUrn(), score, this.getRelated(), this.getId());
  }

  @Override
  public AnomalyEventEntity withRelated(List<? extends Entity> related) {
    return new AnomalyEventEntity(this.getUrn(), this.getScore(), related, this.getId());
  }

  public static AnomalyEventEntity fromDTO(double score, MergedAnomalyResultDTO dto) {
    String urn = TYPE.formatURN(dto.getId());
    return new AnomalyEventEntity(urn, score, new ArrayList<Entity>(), dto.getId());
  }

  public static AnomalyEventEntity fromURN(String urn, double score) {
    ParsedUrn parsedUrn = EntityUtils.parseUrnString(urn, TYPE);
    parsedUrn.assertPrefixOnly();

    long id = Long.parseLong(parsedUrn.getPrefixes().get(3));
    return new AnomalyEventEntity(urn, score, Collections.<Entity>emptyList(), id);
  }
}
