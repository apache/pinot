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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.util.JsonResponseUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path(value = "/thirdeye-admin/dataset-config")
@Produces(MediaType.APPLICATION_JSON)
public class DatasetConfigResource {

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(DatasetConfigResource.class);

  private DatasetConfigManager datasetConfigDAO;

  public DatasetConfigResource() {
    this.datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
  }

  private List<String> toList(String string) {
    String[] splitArray = string.split(",");
    List<String> list = new ArrayList<>();
    for (String split : splitArray) {
      list.add(split.trim());
    }
    return list;
  }

  private void toggleRequiresCompletenessCheck(String dataset, boolean state) {
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
    if(datasetConfig == null) {
      throw new NullArgumentException("dataset config spec not found");
    }
    datasetConfig.setRequiresCompletenessCheck(state);
    datasetConfigDAO.update(datasetConfig);
  }

  @GET
  @Path("/list")
  @Produces(MediaType.APPLICATION_JSON)
  public String viewDatsetConfig(@DefaultValue("0") @QueryParam("jtStartIndex") int jtStartIndex,
      @DefaultValue("100") @QueryParam("jtPageSize") int jtPageSize) {
    List<DatasetConfigDTO> datasetConfigDTOs = datasetConfigDAO.findAll();
    Collections.sort(datasetConfigDTOs, new Comparator<DatasetConfigDTO>() {

      @Override
      public int compare(DatasetConfigDTO d1, DatasetConfigDTO d2) {
        return d1.getDataset().compareTo(d2.getDataset());
      }
    });
    List<DatasetConfigDTO> subList = Utils.sublist(datasetConfigDTOs, jtStartIndex, jtPageSize);
    ObjectNode rootNode = JsonResponseUtil.buildResponseJSON(subList);
    return rootNode.toString();
  }

  @GET
  @Path("/view/{dataset}")
  @Produces(MediaType.APPLICATION_JSON)
  public DatasetConfigDTO viewByDataset(@PathParam("dataset") String dataset) {
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
    return datasetConfig;
  }


  public Long createDatasetConfig(DatasetConfigDTO datasetConfig) {
    Long id = datasetConfigDAO.save(datasetConfig);
    return id;
  }

  public void updateDatasetConfig(DatasetConfigDTO datasetConfig) {
    datasetConfigDAO.update(datasetConfig);
  }

}
