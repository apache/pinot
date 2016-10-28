package com.linkedin.thirdeye.dashboard.resources;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.dashboard.views.IngraphMetricConfigView;
import com.linkedin.thirdeye.datalayer.bao.IngraphMetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.IngraphMetricConfigDTO;

import io.dropwizard.views.View;

@Path(value = "/ingraph-metric-config")
@Produces(MediaType.APPLICATION_JSON)
public class IngraphMetricConfigResource {

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  static ObjectMapper MAPPER = new ObjectMapper();

  private IngraphMetricConfigManager ingraphMetricConfigDao;
  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

  public IngraphMetricConfigResource() {
    this.ingraphMetricConfigDao = DAO_REGISTRY.getIngraphMetricConfigDAO();
  }

  @GET
  @Path(value = "/")
  @Produces(MediaType.TEXT_HTML)
  public View getDashboardView() {
    return new IngraphMetricConfigView();
  }

  @GET
  @Path("/create")
  public String createMetricConfig(@QueryParam("dataset") String dataset,
      @QueryParam("container") String container, @QueryParam("metric") String metric,
      @QueryParam("metricAlias") String metricAlias,
      @QueryParam("metricDataType") String metricDataType,
      @QueryParam("metricSourceType") String metricSourceType,
      @QueryParam("bootstrap") boolean bootstrap, @QueryParam("startTime") String startTime,
      @QueryParam("endTime") String endTime) {
    try {
      IngraphMetricConfigDTO ingraphMetricConfigDTO = new IngraphMetricConfigDTO();
      ingraphMetricConfigDTO.setDataset(dataset);
      ingraphMetricConfigDTO.setContainer(container);
      ingraphMetricConfigDTO.setMetric(metric);
      ingraphMetricConfigDTO.setMetricAlias(metricAlias);
      ingraphMetricConfigDTO.setMetricDataType(metricDataType);
      ingraphMetricConfigDTO.setMetricSourceType(metricSourceType);

      // default values
      String fabrics = "prod-lva1,prod-ltx1,PROD-ELA4,prod-lsg1";
      int granularitySize = 5;
      String granularityUnit = "minutes";
      long numAvroRecords = 100;
      long intervalPeriod = 3600;
      ingraphMetricConfigDTO.setFabrics(fabrics);
      ingraphMetricConfigDTO.setGranularitySize(granularitySize);
      ingraphMetricConfigDTO.setGranularityUnit(granularityUnit);
      ingraphMetricConfigDTO.setMetricSourceType(metricSourceType);
      ingraphMetricConfigDTO.setIntervalPeriod(intervalPeriod);
      ingraphMetricConfigDTO.setNumAvroRecords(numAvroRecords);

      // if bootstrap read start and end
      Boolean needBootstrap = Boolean.valueOf(bootstrap);
      ingraphMetricConfigDTO.setBootstrap(needBootstrap);
      if (needBootstrap) {
        long startTimeInMs;
        startTimeInMs = sdf.parse(startTime).getTime();
        ingraphMetricConfigDTO.setStartTimeInMs(startTimeInMs);
        long endTimeInMs = sdf.parse(endTime).getTime();
        ingraphMetricConfigDTO.setEndTimeInMs(endTimeInMs);
      }
      Long id = ingraphMetricConfigDao.save(ingraphMetricConfigDTO);
      ingraphMetricConfigDTO.setId(id);
      return buildResponseJSON(ingraphMetricConfigDTO).toString();
    } catch (Exception e) {
      return buildErrorResponseJSON("Failed to create metric:" + metric).toString();
    }
  }

  @GET
  @Path("/update")
  public String updateMetricConfig(@NotNull @QueryParam("id") long ingraphMetricConfigId,
      @QueryParam("dataset") String dataset, @QueryParam("container") String container,
      @QueryParam("metric") String metric, @QueryParam("metricAlias") String metricAlias,
      @QueryParam("metricDataType") String metricDataType,
      @QueryParam("metricSourceType") String metricSourceType,
      @QueryParam("bootstrap") boolean bootstrap, @QueryParam("startTime") String startTime,
      @QueryParam("endTime") String endTime) {
    try {

      IngraphMetricConfigDTO ingraphMetricConfigDTO =
          ingraphMetricConfigDao.findById(ingraphMetricConfigId);
      ingraphMetricConfigDTO.setDataset(dataset);
      ingraphMetricConfigDTO.setContainer(container);
      ingraphMetricConfigDTO.setMetric(metric);
      ingraphMetricConfigDTO.setMetricAlias(metricAlias);
      ingraphMetricConfigDTO.setBootstrap(bootstrap);
      // if bootstrap read start and end
      Boolean needBootstrap = Boolean.valueOf(bootstrap);
      ingraphMetricConfigDTO.setBootstrap(needBootstrap);
      if (needBootstrap) {
        long startTimeInMs;
        startTimeInMs = sdf.parse(startTime).getTime();
        ingraphMetricConfigDTO.setStartTimeInMs(startTimeInMs);
        long endTimeInMs = sdf.parse(endTime).getTime();
        ingraphMetricConfigDTO.setEndTimeInMs(endTimeInMs);
      }

      int numRowsUpdated = ingraphMetricConfigDao.update(ingraphMetricConfigDTO);
      if (numRowsUpdated == 1) {
        return buildResponseJSON(ingraphMetricConfigDTO).toString();
      } else {
        return buildErrorResponseJSON("Failed to update metric id:" + ingraphMetricConfigId)
            .toString();
      }
    } catch (Exception e) {
      return buildErrorResponseJSON(
          "Failed to update metric id:" + ingraphMetricConfigId + ". Exception:" + e.getMessage())
              .toString();
    }
  }

  @GET
  @Path("/delete")
  public String deleteMetricConfig(@NotNull @QueryParam("dataset") String dataset,
      @NotNull @QueryParam("id") Long metricConfigId) {
    ingraphMetricConfigDao.deleteById(metricConfigId);
    return buildSuccessResponseJSON("Successully deleted " + metricConfigId).toString();
  }

  @GET
  @Path("/list")
  @Produces(MediaType.APPLICATION_JSON)
  public String viewMetricConfig(@NotNull @QueryParam("dataset") String dataset) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("dataset", dataset);
    List<IngraphMetricConfigDTO> ingraphMetricConfigDTOs =
        ingraphMetricConfigDao.findByParams(filters);
    ObjectNode rootNode = buildResponseJSON(ingraphMetricConfigDTOs);
    return rootNode.toString();
  }

  private ObjectNode buildSuccessResponseJSON(String message) {
    ObjectNode rootNode = MAPPER.getNodeFactory().objectNode();
    rootNode.put("Result", "OK");
    rootNode.put("Message", message);
    return rootNode;
  }

  private ObjectNode buildErrorResponseJSON(String message) {
    ObjectNode rootNode = MAPPER.getNodeFactory().objectNode();
    rootNode.put("Result", "ERROR");
    rootNode.put("Message", message);
    return rootNode;
  }

  private ObjectNode buildResponseJSON(Object obj) {
    ObjectNode rootNode = MAPPER.getNodeFactory().objectNode();
    rootNode.put("Result", "OK");
    JsonNode node = MAPPER.convertValue(obj, JsonNode.class);
    rootNode.put("Record", node);
    return rootNode;
  }

  private ObjectNode buildResponseJSON(List<? extends Object> list) {
    ObjectNode rootNode = MAPPER.getNodeFactory().objectNode();
    ArrayNode resultArrayNode = MAPPER.createArrayNode();
    rootNode.put("Result", "OK");
    for (Object obj : list) {
      JsonNode node = MAPPER.convertValue(obj, JsonNode.class);
      resultArrayNode.add(node);
    }
    rootNode.put("Records", resultArrayNode);
    return rootNode;
  }
}
