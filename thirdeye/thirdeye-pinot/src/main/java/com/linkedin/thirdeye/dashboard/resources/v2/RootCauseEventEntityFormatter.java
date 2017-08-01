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
  public static final String SUFFIX_BASELINE = " (baseline)";

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
    // baseline notification
    if (isRelatedToBaseline(entity))
      label += SUFFIX_BASELINE;

    RootCauseEventEntity out = new RootCauseEventEntity();
    out.setUrn(entity.getUrn());
    out.setScore(Math.round(entity.getScore() * 1000) / 1000.0);
    out.setType("event");
    out.setLabel(label);
    out.setLink(link);
    out.setDetails(details);
    out.setStart(start);
    out.setEnd(end);
    out.setEventType(entity.getEventType());
    return out;
  }

  /**
   * Returns {@code true} if the entity is related to at least on TimeRangeEntity of type
   * {@code TYPE_BASELINE}, otherwise returns {@code false}.
   *
   * @param e entity
   * @return {@code true} if the entity is related to the baseline, {@code false} otherwise.
   */
  public static boolean isRelatedToBaseline(Entity e) {
    for(Entity re : e.getRelated()) {
      if(TimeRangeEntity.TYPE.isType(re))
        if(TimeRangeEntity.fromURN(re.getUrn(), re.getScore()).getType().equals(TimeRangeEntity.TYPE_BASELINE))
          return true;
    }
    return false;
  }
}
