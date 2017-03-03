package com.linkedin.thirdeye.dashboard.resources;

import com.google.common.base.Strings;

import java.util.ArrayList;
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

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.util.JsonResponseUtil;

@Path(value = "/thirdeye-admin/dataset-config")
@Produces(MediaType.APPLICATION_JSON)
public class DatasetConfigResource {

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private DatasetConfigManager datasetConfigDao;

  public DatasetConfigResource() {
    this.datasetConfigDao = DAO_REGISTRY.getDatasetConfigDAO();
  }

  @GET
  @Path("/create")
  public String createDatasetConfig(@QueryParam("dataset") String dataset, @QueryParam("dimensions") String dimensions,
      @QueryParam("dimensionsHaveNoPreAggregation") String dimensionsHaveNoPreAggregation, @QueryParam("active") boolean active,
      @QueryParam("additive") boolean additive, @QueryParam("metricAsDimension") boolean metricAsDimension,
      @QueryParam("metricValuesColumn") String metricValuesColumn, @QueryParam("metricNamesColumn") String metricNamesColumn,
      @QueryParam("nonAdditiveBucketSize") Integer nonAdditiveBucketSize, @QueryParam("nonAdditiveBucketUnit") String nonAdditiveBucketUnit,
      @QueryParam("preAggregatedKeyword") String preAggregatedKeyword, @QueryParam("timeColumn") String timeColumn,
      @QueryParam("timeDuration") Integer timeDuration, @QueryParam("timeFormat") String timeFormat, @QueryParam("timezone") TimeUnit timeUnit,
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
      datasetConfigDTO.setMetricAsDimension(metricAsDimension);
      datasetConfigDTO.setMetricNamesColumn(metricNamesColumn);
      datasetConfigDTO.setMetricValuesColumn(metricValuesColumn);
      datasetConfigDTO.setNonAdditiveBucketSize(nonAdditiveBucketSize);
      datasetConfigDTO.setNonAdditiveBucketUnit(nonAdditiveBucketUnit);
      datasetConfigDTO.setPreAggregatedKeyword(preAggregatedKeyword);
      datasetConfigDTO.setTimeColumn(timeColumn);
      datasetConfigDTO.setTimeDuration(timeDuration);
      datasetConfigDTO.setTimeFormat(timeFormat);
      datasetConfigDTO.setTimeUnit(timeUnit);
      datasetConfigDTO.setTimezone(timezone);
      Long id = datasetConfigDao.save(datasetConfigDTO);
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
  public String updateDatasetConfig(@NotNull @QueryParam("id") long datasetConfigId, @QueryParam("dataset") String dataset,
      @QueryParam("dimensions") String dimensions, @QueryParam("dimensionsHaveNoPreAggregation") String dimensionsHaveNoPreAggregation,
      @QueryParam("active") boolean active, @QueryParam("additive") boolean additive, @QueryParam("metricAsDimension") boolean metricAsDimension,
      @QueryParam("metricValuesColumn") String metricValuesColumn, @QueryParam("metricNamesColumn") String metricNamesColumn,
      @QueryParam("nonAdditiveBucketSize") Integer nonAdditiveBucketSize, @QueryParam("nonAdditiveBucketUnit") String nonAdditiveBucketUnit,
      @QueryParam("preAggregatedKeyword") String preAggregatedKeyword, @QueryParam("timeColumn") String timeColumn,
      @QueryParam("timeDuration") Integer timeDuration, @QueryParam("timeFormat") String timeFormat, @QueryParam("timezone") TimeUnit timeUnit,
      @QueryParam("timezone") String timezone) {
    try {
      DatasetConfigDTO datasetConfigDTO = datasetConfigDao.findById(datasetConfigId);
      datasetConfigDTO.setDataset(dataset);
      datasetConfigDTO.setDimensions(toList(dimensions));
      datasetConfigDTO.setDimensionsHaveNoPreAggregation(toList(dimensionsHaveNoPreAggregation));
      datasetConfigDTO.setActive(active);
      datasetConfigDTO.setAdditive(additive);
      datasetConfigDTO.setMetricAsDimension(metricAsDimension);
      datasetConfigDTO.setMetricValuesColumn(metricValuesColumn);
      datasetConfigDTO.setNonAdditiveBucketSize(nonAdditiveBucketSize);
      datasetConfigDTO.setNonAdditiveBucketUnit(nonAdditiveBucketUnit);
      datasetConfigDTO.setPreAggregatedKeyword(preAggregatedKeyword);
      datasetConfigDTO.setTimeColumn(timeColumn);
      datasetConfigDTO.setTimeDuration(timeDuration);
      datasetConfigDTO.setTimeFormat(timeFormat);
      datasetConfigDTO.setTimeUnit(timeUnit);
      datasetConfigDTO.setTimezone(timezone);
      int numRowsUpdated = datasetConfigDao.update(datasetConfigDTO);
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
    DatasetConfigDTO datasetConfig = datasetConfigDao.findByDataset(dataset);
    if(datasetConfig == null) {
      throw new NullArgumentException("dataset config spec not found");
    }
    datasetConfig.setRequiresCompletenessCheck(state);
    datasetConfigDao.update(datasetConfig);
  }

  @GET
  @Path("/delete")
  public String deleteDatasetConfig(@NotNull @QueryParam("id") Long datasetConfigId) {
    datasetConfigDao.deleteById(datasetConfigId);
    return JsonResponseUtil.buildSuccessResponseJSON("Successully deleted dataset id: " + datasetConfigId).toString();
  }

  @GET
  @Path("/list")
  @Produces(MediaType.APPLICATION_JSON)
  public String viewDatsetConfig(@DefaultValue("0") @QueryParam("jtStartIndex") int jtStartIndex,
      @DefaultValue("100") @QueryParam("jtPageSize") int jtPageSize) {
    List<DatasetConfigDTO> datasetConfigDTOs = datasetConfigDao.findAll();
    List<DatasetConfigDTO> subList = Utils.sublist(datasetConfigDTOs, jtStartIndex, jtPageSize);
    ObjectNode rootNode = JsonResponseUtil.buildResponseJSON(subList);
    return rootNode.toString();
  }





}
