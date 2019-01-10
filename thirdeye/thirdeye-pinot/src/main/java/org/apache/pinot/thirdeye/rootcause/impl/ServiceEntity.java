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
import org.apache.pinot.thirdeye.rootcause.util.EntityUtils;
import org.apache.pinot.thirdeye.rootcause.util.ParsedUrn;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * ServiceEntity represents a service associated with certain metrics or dimensions. It typically
 * serves as a connecting piece between observed discrepancies between current and baseline metrics
 * and root cause events such as code deployments. The URN namespace is defined as
 * 'thirdeye:service:{name}'.
 */
public class ServiceEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:service:");

  private final String name;

  protected ServiceEntity(String urn, double score, List<? extends Entity> related, String name) {
    super(urn, score, related);
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public ServiceEntity withScore(double score) {
    return new ServiceEntity(this.getUrn(), score, this.getRelated(), this.name);
  }

  @Override
  public ServiceEntity withRelated(List<? extends Entity> related) {
    return new ServiceEntity(this.getUrn(), this.getScore(), related, this.name);
  }

  public static ServiceEntity fromName(double score, String name) {
    String urn = TYPE.formatURN(name);
    return new ServiceEntity(urn, score, new ArrayList<Entity>(), name);
  }

  public static ServiceEntity fromURN(String urn, double score) {
    ParsedUrn parsedUrn = EntityUtils.parseUrnString(urn, TYPE);
    parsedUrn.assertPrefixOnly();

    String service = parsedUrn.getPrefixes().get(2);
    return new ServiceEntity(urn, score, Collections.<Entity>emptyList(), service);
  }
}
