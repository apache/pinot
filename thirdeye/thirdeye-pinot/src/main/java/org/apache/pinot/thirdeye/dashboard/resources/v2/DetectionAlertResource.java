/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.dashboard.resources.v2;

import com.google.inject.Inject;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import io.swagger.annotations.Api;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;


/**
 * Endpoints for detection alert (aka "groups") management
 */
@Api(tags = {Constants.DASHBOARD_TAG})
@Produces(MediaType.APPLICATION_JSON)
public class DetectionAlertResource {
  private final DetectionAlertConfigManager detectionAlertDAO;

  @Inject
  public DetectionAlertResource(DetectionAlertConfigManager detectionAlertDAO) {
    this.detectionAlertDAO = detectionAlertDAO;
  }

  @GET
  @Path("{id}")
  public DetectionAlertConfigDTO get(@PathParam("id") Long id) {
    return this.detectionAlertDAO.findById(id);
  }

  @GET
  @Path("batch")
  public List<DetectionAlertConfigDTO> getBatch(@QueryParam("ids") List<String> ids) {
    return this.detectionAlertDAO.findByIds(ResourceUtils.parseListParamsLong(ids));
  }

  @GET
  @Path("query")
  public List<Long> query(
      @QueryParam("id") List<String> id,
      @QueryParam("name") List<String> name,
      @QueryParam("from") List<String> from,
      @QueryParam("application") List<String> application) {

    // build query
    List<Predicate> predicates = new ArrayList<>();

    if (!id.isEmpty()) {
      predicates.add(Predicate.IN("id", ResourceUtils.parseListParamsLong(id).toArray()));
    }

    if (!name.isEmpty()) {
      predicates.add(Predicate.OR(makeLike("name", ResourceUtils.parseListParams(name))));
    }

    if (!from.isEmpty()) {
      predicates.add(Predicate.OR(makeLike("from", ResourceUtils.parseListParams(from))));
    }

    if (!application.isEmpty()) {
      predicates.add(Predicate.OR(makeLike("application", ResourceUtils.parseListParams(application))));
    }

    // fetch
    if (predicates.isEmpty()) {
      return Collections.emptyList();
    }

    return this.detectionAlertDAO.findIdsByPredicate(Predicate.AND(predicates.toArray(new Predicate[0])));
  }

  private static Predicate[] makeLike(String column, List<String> values) {
    List<Predicate> children = new ArrayList<>();
    for (String value : values) {
      children.add(Predicate.LIKE(column, String.format("%%%s%%", value)));
    }
    return children.toArray(new Predicate[0]);
  }
}
