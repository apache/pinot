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


public class EventEntityFormatter extends RootCauseEntityFormatter {
  private static final String TYPE_HOLIDAY = "HOLIDAY";

  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;

  public EventEntityFormatter(EventManager eventDAO, MergedAnomalyResultManager anomalyDAO) {
    this.eventDAO = eventDAO;
    this.anomalyDAO = anomalyDAO;
  }

  public EventEntityFormatter() {
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
  }

  @Override
  public boolean applies(Entity entity) {
    return EventEntity.TYPE.isType(entity.getUrn());
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    EventEntity e = EventEntity.fromURN(entity.getUrn(), entity.getScore());

    String label = String.format("Other (%s %d)", e.getType(), e.getId());
    if(TYPE_HOLIDAY.equals(e.getType()))
      label = formatHolidayLabel(e);

    String link = String.format("javascript:alert('%s');", entity.getUrn());

    return makeRootCauseEntity(entity, "Event", label, link);
  }

  private String formatHolidayLabel(EventEntity e) {
    EventDTO dto = this.eventDAO.findById(e.getId());
    if(dto == null)
      return "Unknown";
    return String.format("%s %d", dto.getName(), new DateTime(dto.getStartTime()).getYear());
  }
}
