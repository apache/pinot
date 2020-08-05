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

package org.apache.pinot.thirdeye.dashboard.resources;

import com.google.inject.Inject;
import io.dropwizard.views.View;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.dashboard.views.ThirdEyeAdminView;

@Api(tags = {Constants.ADMIN_TAG})
@Produces(MediaType.APPLICATION_JSON)
public class AdminResource {

  private final DatasetConfigResource datasetConfigResource;

  @Inject
  public AdminResource(
      final DatasetConfigResource datasetConfigResource) {
    this.datasetConfigResource = datasetConfigResource;
  }

  @Path("dataset-config")
  public DatasetConfigResource getDatasetConfigResource() {
    return datasetConfigResource;
  }

  @GET
  @Path(value = "/")
  @ApiOperation(value = "Load the ThirdEye admin dashboard.")
  @Produces(MediaType.TEXT_HTML)
  public View getDashboardView() {
    return new ThirdEyeAdminView();
  }
}
