package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;

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
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.util.JsonResponseUtil;

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

  @GET
  @Path("/create")
  public String createDatasetConfig(
      @QueryParam("dataset") String dataset,
      @QueryParam("dimensions") String dimensions,
      @QueryParam("dimensionsHaveNoPreAggregation") String dimensionsHaveNoPreAggregation,
      @QueryParam("active") boolean active,
      @QueryParam("additive") boolean additive,
      @QueryParam("nonAdditiveBucketSize") Integer nonAdditiveBucketSize,
      @QueryParam("nonAdditiveBucketUnit") String nonAdditiveBucketUnit,
      @QueryParam("preAggregatedKeyword") String preAggregatedKeyword,
      @QueryParam("timeColumn") String timeColumn,
      @QueryParam("timeDuration") Integer timeDuration,
      @QueryParam("timeFormat") String timeFormat,
      @QueryParam("timezone") TimeUnit timeUnit,
      @QueryParam("timezone") String timezone) {
    try {
      DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
      datasetConfigDTO.setDataset(dataset);
      datasetConfigDTO.setDimensions(toList(dimensions));
      if (!Strings.isNullOrEmpty(dimensionsHaveNoPreAggregation)) {
        datasetConfigDTO.setDimensionsHaveNoPreAggregation(toList(dimensionsHaveNoPreAggregation));
      }
      datasetConfigDTO.setActive(active);
      datasetConfigDTO.setAdditive(additive);
      datasetConfigDTO.setNonAdditiveBucketSize(nonAdditiveBucketSize);
      datasetConfigDTO.setNonAdditiveBucketUnit(TimeUnit.valueOf(nonAdditiveBucketUnit));
      datasetConfigDTO.setPreAggregatedKeyword(preAggregatedKeyword);
      datasetConfigDTO.setTimeColumn(timeColumn);
      datasetConfigDTO.setTimeDuration(timeDuration);
      datasetConfigDTO.setTimeFormat(timeFormat);
      datasetConfigDTO.setTimeUnit(timeUnit);
      datasetConfigDTO.setTimezone(timezone);
      Long id = datasetConfigDAO.save(datasetConfigDTO);
      datasetConfigDTO.setId(id);
      return JsonResponseUtil.buildResponseJSON(datasetConfigDTO).toString();
    } catch (Exception e) {
      return JsonResponseUtil.buildErrorResponseJSON("Failed to create dataset:" + dataset).toString();
    }
  }

  private List<String> toList(String string) {
    String[] splitArray = string.split(",");
    List<String> list = new ArrayList<>();
    for (String split : splitArray) {
      list.add(split.trim());
    }
    return list;
  }

  @GET
  @Path("/update")
  public String updateDatasetConfig(
      @NotNull @QueryParam("id") long datasetConfigId,
      @QueryParam("dataset") String dataset,
      @QueryParam("dimensions") String dimensions,
      @QueryParam("dimensionsHaveNoPreAggregation") String dimensionsHaveNoPreAggregation,
      @QueryParam("active") boolean active,
      @QueryParam("additive") boolean additive,
      @QueryParam("nonAdditiveBucketSize") Integer nonAdditiveBucketSize,
      @QueryParam("nonAdditiveBucketUnit") String nonAdditiveBucketUnit,
      @QueryParam("preAggregatedKeyword") String preAggregatedKeyword,
      @QueryParam("timeColumn") String timeColumn,
      @QueryParam("timeDuration") Integer timeDuration,
      @QueryParam("timeFormat") String timeFormat,
      @QueryParam("timezone") TimeUnit timeUnit,
      @QueryParam("timezone") String timezone) {
    try {
      DatasetConfigDTO datasetConfigDTO = datasetConfigDAO.findById(datasetConfigId);
      datasetConfigDTO.setDataset(dataset);
      datasetConfigDTO.setDimensions(toList(dimensions));
      datasetConfigDTO.setDimensionsHaveNoPreAggregation(toList(dimensionsHaveNoPreAggregation));
      datasetConfigDTO.setActive(active);
      datasetConfigDTO.setAdditive(additive);
      datasetConfigDTO.setNonAdditiveBucketSize(nonAdditiveBucketSize);
      datasetConfigDTO.setNonAdditiveBucketUnit(TimeUnit.valueOf(nonAdditiveBucketUnit));
      datasetConfigDTO.setPreAggregatedKeyword(preAggregatedKeyword);
      datasetConfigDTO.setTimeColumn(timeColumn);
      datasetConfigDTO.setTimeDuration(timeDuration);
      datasetConfigDTO.setTimeFormat(timeFormat);
      datasetConfigDTO.setTimeUnit(timeUnit);
      datasetConfigDTO.setTimezone(timezone);
      int numRowsUpdated = datasetConfigDAO.update(datasetConfigDTO);
      if (numRowsUpdated == 1) {
        return JsonResponseUtil.buildResponseJSON(datasetConfigDTO).toString();
      } else {
        return JsonResponseUtil.buildErrorResponseJSON("Failed to update dataset config id:" + datasetConfigId).toString();
      }
    } catch (Exception e) {
      return JsonResponseUtil.buildErrorResponseJSON("Failed to update dataset config id:" + datasetConfigId + ". Exception:" + e.getMessage()).toString();
    }
  }


  @POST
  @Path("/requiresCompletenessCheck/enable/{dataset}")
  public Response enableRequiresCompletenessCheck(@PathParam("dataset") String dataset) throws Exception {
    toggleRequiresCompletenessCheck(dataset, true);
    return Response.ok().build();
  }

  @POST
  @Path("/requiresCompletenessCheck/disable/{dataset}")
  public Response disableRequiresCompletenessCheck(@PathParam("dataset") String dataset) throws Exception {
    toggleRequiresCompletenessCheck(dataset, false);
    return Response.ok().build();
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
  @Path("/delete")
  public String deleteDatasetConfig(@NotNull @QueryParam("id") Long datasetConfigId) {
    datasetConfigDAO.deleteById(datasetConfigId);
    return JsonResponseUtil.buildSuccessResponseJSON("Successully deleted dataset id: " + datasetConfigId).toString();
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


  @POST
  @Path("/create/payload")
  public Long createDatasetConfig(String payload) {
    Long id = null;
    try {
      DatasetConfigDTO datasetConfig = OBJECT_MAPPER.readValue(payload, DatasetConfigDTO.class);
      id = datasetConfigDAO.save(datasetConfig);
    } catch (IOException e) {
      LOG.error("Exception in creating dataset config with payload {}", payload);
    }
    return id;
  }

  public Long createDatasetConfig(DatasetConfigDTO datasetConfig) {
    Long id = datasetConfigDAO.save(datasetConfig);
    return id;
  }

  public void updateDatasetConfig(DatasetConfigDTO datasetConfig) {
    datasetConfigDAO.update(datasetConfig);
  }

}
