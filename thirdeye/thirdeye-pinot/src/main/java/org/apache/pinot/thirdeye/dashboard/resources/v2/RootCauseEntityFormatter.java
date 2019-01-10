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

package org.apache.pinot.thirdeye.dashboard.resources.v2;

import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import org.apache.pinot.thirdeye.rootcause.Entity;


/**
 * Foundation class for building UI formatters for RCA Entities. Takes in RCA Entities and returns
 * RootCauseEntity container instances that contain human-readable data for display on the GUI.
 */
public abstract class RootCauseEntityFormatter {
  public abstract boolean applies(Entity entity);

  public abstract RootCauseEntity format(Entity entity);

  public static RootCauseEntity makeRootCauseEntity(Entity entity, String type, String label, String link) {
    RootCauseEntity out = new RootCauseEntity();
    out.setUrn(entity.getUrn());
    out.setScore(entity.getScore());
    out.setType(type);
    out.setLabel(label);
    out.setLink(link);
    return out;
  }
}
