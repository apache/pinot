package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEventEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEventEntity;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.impl.DimensionEntity;
import com.linkedin.thirdeye.rootcause.impl.EventEntity;
import com.linkedin.thirdeye.rootcause.impl.HolidayEventEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class HolidayEventFormatter extends RootCauseEventEntityFormatter {
  private static final DimensionEntityFormatter DIMENSION_FORMATTER = new DimensionEntityFormatter();

  @Override
  public boolean applies(EventEntity entity) {
    return entity instanceof HolidayEventEntity;
  }

  @Override
  public RootCauseEventEntity format(EventEntity entity) {
    HolidayEventEntity holidayEvent = (HolidayEventEntity) entity;
    EventDTO eventDto = holidayEvent.getDto();

    StringBuilder dimensions = new StringBuilder();
    List<DimensionEntity> relatedDimensionEntities = new ArrayList<>();
    for(Map.Entry<String, List<String>> entry : eventDto.getTargetDimensionMap().entrySet()) {
      String dimName = entry.getKey();
      dimensions.append(" (" + dimName + ":");
      dimensions.append(Joiner.on(",").join(entry.getValue()) + ")");
      for(String dimValue : entry.getValue()) {
        DimensionEntity de = DimensionEntity.fromDimension(entity.getScore(), dimName, dimValue, DimensionEntity.TYPE_GENERATED);
        relatedDimensionEntities.add(de);
      }
    }

    String label = String.format("%s %s", eventDto.getName(), dimensions.toString());
    String link =  String.format("https://www.google.com/search?q=%s", eventDto.getName());
    RootCauseEventEntity out = makeRootCauseEventEntity(entity, label, link, eventDto.getStartTime(), eventDto.getEndTime(), "");

    for (DimensionEntity dimensionEntity : relatedDimensionEntities) {
      out.addRelatedEntity(DIMENSION_FORMATTER.format(dimensionEntity));
    }

    return out;
  }
}
