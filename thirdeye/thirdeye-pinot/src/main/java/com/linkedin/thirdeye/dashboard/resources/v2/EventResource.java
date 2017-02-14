package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.anomaly.events.DefaultHolidayEventProvider;
import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path(value = "/event")
@Produces(MediaType.APPLICATION_JSON)
public class EventResource {

  public static final EventDataProviderManager EVENT_DATA_PROVIDER_MANAGER =  EventDataProviderManager.getInstance();

  public EventResource() {
    EVENT_DATA_PROVIDER_MANAGER
        .registerEventDataProvider(EventType.HOLIDAY, new DefaultHolidayEventProvider());
  }

  @GET
  @Path ("/{start}/{end}")
  public List<EventDTO> getHolidayEventsByTime(@PathParam("start") long start, @PathParam("end") long end) {
    EventFilter eventFilter = new EventFilter();
    eventFilter.setStartTime(start);
    eventFilter.setEndTime(end);
    eventFilter.setEventType(EventType.HOLIDAY);
    return EVENT_DATA_PROVIDER_MANAGER.getEvents(eventFilter);
  }

  @POST
  @Path("/")
  public List<EventDTO> getEventsByFilter(EventFilter eventFilter) {
    return EVENT_DATA_PROVIDER_MANAGER.getEvents(eventFilter);
  }
}
