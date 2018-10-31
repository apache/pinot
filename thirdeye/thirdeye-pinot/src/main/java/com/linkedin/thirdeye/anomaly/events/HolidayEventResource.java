package com.linkedin.thirdeye.anomaly.events;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;


/**
 * The Holiday event resource.
 */
@Path(value = "/holidays")
public class HolidayEventResource {

  /**
   * The Events loader.
   */
  HolidayEventsLoader eventsLoader;

  /**
   * Instantiates a new Holiday event resource.
   *
   * @param loader the loader
   */
  public HolidayEventResource(HolidayEventsLoader loader) {
    this.eventsLoader = loader;
  }

  /**
   * Load the holidays between startTime and endTime to Third Eye database.
   *
   * @param startTime the start time
   * @param endTime the end time
   */
  @POST
  @Path("/load")
  public void loadHolidays(
      @QueryParam("startTime") long startTime, @QueryParam("endTime") long endTime
  ) {
    eventsLoader.loadHolidays(startTime, endTime);
  }
}
