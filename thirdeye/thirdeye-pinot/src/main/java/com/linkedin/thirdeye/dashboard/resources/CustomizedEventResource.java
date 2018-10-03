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

package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.api.Constants;
import com.linkedin.thirdeye.dashboard.resources.v2.ResourceUtils;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;


/**
 * Customized events resource.
 */
@Path(value = "/events")
@Api(tags = {Constants.RCA_TAG})
public class CustomizedEventResource {

  private final EventManager eventDAO;

  /**
   * Instantiates a new Customized events resource.
   *
   * @param eventDAO the event dao
   */
  public CustomizedEventResource(EventManager eventDAO) {
    this.eventDAO = eventDAO;
  }

  /**
   * Create a customized event.
   *
   * @param eventName the event name
   * @param startTime the event start time
   * @param endTime the event end time
   */
  @POST
  @Path("/create")
  @ApiOperation(value = "Create a customized event.")
  public void createCustomizedEvent(@ApiParam(required = true) @NotNull @QueryParam("eventName") String eventName,
      @QueryParam("startTime") long startTime, @QueryParam("endTime") long endTime, @QueryParam("countryCode") List<String> countryCode
      ) {
    EventDTO eventDTO = new EventDTO();
    eventDTO.setName(eventName);
    eventDTO.setStartTime(startTime);
    eventDTO.setEndTime(endTime);
    eventDTO.setEventType(EventType.CUSTOM.toString());

    Map<String, List<String>> targetDimensionMap = new HashMap<>();
    targetDimensionMap.put("countryCode", ResourceUtils.parseListParams(countryCode));
    eventDTO.setTargetDimensionMap(targetDimensionMap);

    eventDAO.save(eventDTO);
  }
}
