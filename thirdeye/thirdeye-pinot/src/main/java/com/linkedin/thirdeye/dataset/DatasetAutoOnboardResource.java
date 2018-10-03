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

package com.linkedin.thirdeye.dataset;

import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;


@Path("/dataset-auto-onboard")
@Produces(MediaType.APPLICATION_JSON)
public class DatasetAutoOnboardResource {
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private final MetricConfigManager metricDAO;
  private final DetectionConfigManager detectionDAO;
  private final DatasetConfigManager datasetDAO;

  public DatasetAutoOnboardResource() {
    this.metricDAO = DAO_REGISTRY.getMetricConfigDAO();
    this.detectionDAO = DAO_REGISTRY.getDetectionConfigManager();
    this.datasetDAO = DAO_REGISTRY.getDatasetConfigDAO();
  }

  @GET
  @Path("/metrics")
  public List<MetricConfigDTO> detectionPreview(@QueryParam("dataset") String dataSet) {
    return this.metricDAO.findByDataset(dataSet);
  }

  @GET
  @Path("/{detectionId}")
  public DetectionConfigDTO getDetectionConfig(@PathParam("detectionId") long detectionId) {
    return this.detectionDAO.findById(detectionId);
  }
}

