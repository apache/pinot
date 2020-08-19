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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.auth.ThirdEyeAuthFilter;
import org.apache.pinot.thirdeye.dashboard.resources.v2.rootcause.DimensionAnalysisModuleConfig;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.RootcauseTemplateDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import scala.Array;


@Path(value = "/rootcause/template")
@Api(tags = { Constants.RCA_TAG })
@Produces(MediaType.APPLICATION_JSON)
public class RootCauseTemplateResource {
  private final static String DIM_ANALYSIS_TEMPLATE_NAME_PREFIX = "dim_analysis::";
  private final static String DIM_ANALYSIS_MODULE_NAME_PREFIX = "dim_analysis::";
  private final static String POC_APPLICATION = "thirdeye-poc";
  private final static String RCA_MODULE_NAME = "name";
  private final static String RCA_MODULE_CONFIG = "configuration";
  private final static DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  /**
   * Search RCA template based on metric ID, and it should be expanded to support different query later.
   * @param metricId metric ID
   * @return list of RCA templates
   */
  @GET
  @Path("/search")
  @ApiOperation(value = "Get root cause template")
  public List<RootcauseTemplateDTO> get(
      @QueryParam("metricId") Long metricId) {
    if (metricId == null) {
      throw new IllegalArgumentException("Must provide valid metricId");
    }
    List<RootcauseTemplateDTO> templates = DAO_REGISTRY.getRootcauseTemplateDao().findByMetricId(metricId);
    if (templates == null) {
      templates = new ArrayList<>();
    }
    return templates;
  }

  /**
   * create a RCA template for single dimensional analysis module for RCA v2 POC.
   * @param metricUrn metricURN for the template
   * @param dimensionStr comma separated string of list of included dimensions
   * @param excludeDimStr comma separated string of list of excluded dimensions
   * @param manualOrder flag to maintain order of included dimension direction
   * @param oneSideError flag to show dimension candidates with different direction
   * @param summarySize number of dimension candidates to be shown
   * @param dimensionDepth number of dimensions to be explored
   * @return id for the saved RCA template
   */
  @POST
  @Path("/saveDimensionAnalysis")
  public Long saveDimensionAnalysis(
      @QueryParam("metricUrn") String metricUrn,
      @QueryParam("includedDimension") String dimensionStr,
      @QueryParam("excludedDimension") String excludeDimStr,
      @QueryParam("manualOrder") boolean manualOrder,
      @QueryParam("oneSideError") boolean oneSideError,
      @QueryParam("summarySize") int summarySize,
      @QueryParam("dimensionDepth") int dimensionDepth
  ) {
    ObjectMapper objMapper = new ObjectMapper();
    final String username = ThirdEyeAuthFilter.getCurrentPrincipal().getName();
    MetricEntity metricEntity = MetricEntity.fromURN(metricUrn);
    MetricConfigManager metricConfigDao = DAO_REGISTRY.getMetricConfigDAO();
    MetricConfigDTO metricConfigDTO = metricConfigDao.findById(metricEntity.getId());
    String templateName = DIM_ANALYSIS_TEMPLATE_NAME_PREFIX + metricConfigDTO.getAlias();
    RootcauseTemplateDTO rootcauseTemplateDTO = new RootcauseTemplateDTO();
    rootcauseTemplateDTO.setName(templateName);
    rootcauseTemplateDTO.setOwner(username);
    rootcauseTemplateDTO.setMetricId(metricEntity.getId());
    rootcauseTemplateDTO.setApplication(POC_APPLICATION);
    List<Map<String, Object>> modules = new ArrayList<>();
    Map<String, Object> dimAnalysisModule = new HashMap<>();
    dimAnalysisModule.put(RCA_MODULE_NAME, DIM_ANALYSIS_MODULE_NAME_PREFIX + metricConfigDTO.getAlias());
    DimensionAnalysisModuleConfig dimAnalysisModuleConfig = new DimensionAnalysisModuleConfig();
    if (dimensionStr != null  && !dimensionStr.isEmpty()) {
      dimAnalysisModuleConfig.setIncludedDimension(Arrays.asList(dimensionStr.split(",")));
    } else {
      dimAnalysisModuleConfig.setIncludedDimension(Collections.emptyList());
    }
    if (excludeDimStr != null  && !excludeDimStr.isEmpty()) {
      dimAnalysisModuleConfig.setIncludedDimension(Arrays.asList(excludeDimStr.split(",")));
    } else {
      dimAnalysisModuleConfig.setExcludedDimension(Collections.emptyList());
    }
    dimAnalysisModuleConfig.setManualOrder(manualOrder);
    dimAnalysisModuleConfig.setOneSideError(oneSideError);
    dimAnalysisModuleConfig.setSummarySize(summarySize);
    dimAnalysisModuleConfig.setDimensionDepth(dimensionDepth);
    dimAnalysisModule.put(RCA_MODULE_CONFIG, objMapper.convertValue(dimAnalysisModuleConfig, Map.class));
    modules.add(dimAnalysisModule);
    rootcauseTemplateDTO.setModules(modules);
    return DAO_REGISTRY.getRootcauseTemplateDao().saveOrUpdate(rootcauseTemplateDTO);
  }

}
