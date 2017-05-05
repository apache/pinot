package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.impl.EventEntity;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class HolidayFormatter extends RootCauseEntityFormatter {
  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forStyle("S-");

  private static final String TYPE_HOLIDAY = "HOLIDAY";

  private final EventManager eventDAO;

  public HolidayFormatter(EventManager eventDAO) {
    this.eventDAO = eventDAO;
  }

  public HolidayFormatter() {
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
  }

  @Override
  public boolean applies(Entity entity) {
    return entity.getUrn().startsWith(EventEntity.TYPE.getPrefix() + TYPE_HOLIDAY + ":");
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    EventEntity e = EventEntity.fromURN(entity.getUrn(), entity.getScore());

    String label = formatHolidayLabel(e);
    String link =  String.format("https://www.google.com/search?q=%s", label);

    return makeRootCauseEntity(entity, TYPE_HOLIDAY, label, link);
  }

  private String formatHolidayLabel(EventEntity e) {
    EventDTO dto = this.eventDAO.findById(e.getId());
    if(dto == null)
      return "Unknown";
    return String.format("(%s) %s", DATE_FORMAT.print(dto.getStartTime()), dto.getName());
  }
}
