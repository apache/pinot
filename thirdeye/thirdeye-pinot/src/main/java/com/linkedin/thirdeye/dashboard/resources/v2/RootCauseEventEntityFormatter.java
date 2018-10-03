/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEventEntity;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.impl.EventEntity;
import com.linkedin.thirdeye.rootcause.impl.TimeRangeEntity;


/**
 * Foundation class for building UI formatters for RCA Entities. Takes in RCA Entities and returns
 * RootCauseEntity container instances that contain human-readable data for display on the GUI.
 */
public abstract class RootCauseEventEntityFormatter extends RootCauseEntityFormatter {
  @Override
  public final boolean applies(Entity entity) {
    if(!(entity instanceof EventEntity))
      return false;
    return this.applies((EventEntity) entity);
  }

  @Override
  public final RootCauseEntity format(Entity entity) {
    return this.format((EventEntity) entity);
  }

  public abstract boolean applies(EventEntity entity);

  public abstract RootCauseEventEntity format(EventEntity entity);

  public static RootCauseEventEntity makeRootCauseEventEntity(EventEntity entity, String label, String link, long start, long end, String details) {
    RootCauseEventEntity out = new RootCauseEventEntity();
    out.setUrn(entity.getUrn());
    out.setScore(entity.getScore());
    out.setType("event");
    out.setLabel(label);
    out.setLink(link);
    out.setDetails(details);
    out.setStart(start);
    out.setEnd(end);
    out.setEventType(entity.getEventType());
    return out;
  }
}
