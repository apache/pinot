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
