package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEventEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEventEntity;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.impl.DimensionEntity;
import com.linkedin.thirdeye.rootcause.impl.EventEntity;
import com.linkedin.thirdeye.rootcause.impl.HolidayEventEntity;
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
    HolidayEventEntity e = (HolidayEventEntity) entity;

    EventDTO dto = e.getDto();
    String link =  String.format("https://www.google.com/search?q=%s", dto.getName());

    RootCauseEventEntity out = makeRootCauseEventEntity(entity, dto.getName(), link, dto.getStartTime(), dto.getEndTime(), "");

    for(Map.Entry<String, List<String>> entry : dto.getTargetDimensionMap().entrySet()) {
      String dimName = entry.getKey();
      for(String dimValue : entry.getValue()) {
        DimensionEntity de = DimensionEntity.fromDimension(1.0, dimName, dimValue);
        out.addRelatedEntity(DIMENSION_FORMATTER.format(de));
      }
    }

    return out;
  }
}
