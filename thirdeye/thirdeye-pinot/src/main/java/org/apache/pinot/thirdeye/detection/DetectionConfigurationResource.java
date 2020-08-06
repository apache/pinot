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
 *
 */

package org.apache.pinot.thirdeye.detection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Singleton;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;


@Api(tags = {Constants.DETECTION_TAG})
@Singleton
public class DetectionConfigurationResource {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static DetectionRegistry detectionRegistry = DetectionRegistry.getInstance();

  private static String TUNABLE_TYPE = "TUNABLE";
  private static String BASELINE_TYPE = "BASELINE";

  @GET
  public Response getRules() throws Exception {
    List<Components> componentsList = detectionRegistry.getAllAnnotation();
    componentsList = componentsList.stream().filter(component -> {
      String type = component.type().toUpperCase();
      return !type.contains(TUNABLE_TYPE) && !type.contains(BASELINE_TYPE);
    }).collect(Collectors.toList());
    return Response.ok(OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(componentsList)).build();
  }
}
