package com.linkedin.thirdeye.dashboard.resources;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.util.JsonResponseUtil;

@Path(value = "/thirdeye-admin/metric-config")
@Produces(MediaType.APPLICATION_JSON)
public class MetricConfigResource {

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private MetricConfigManager metricConfigDao;

  public MetricConfigResource() {
    this.metricConfigDao = DAO_REGISTRY.getMetricConfigDAO();
  }

  @GET
  @Path("/create")
  public String createMetricConfig(@QueryParam("dataset") String dataset, @QueryParam("name") String name, @QueryParam("alias") String alias,
      @QueryParam("metricType") String metricType, @QueryParam("active") boolean active, @QueryParam("derived") boolean derived,
      @QueryParam("functionType") String derivedFunctionType, @QueryParam("numerator") String numerator, @QueryParam("denominator") String denominator,
      @QueryParam("derivedMetricExpression") String derivedMetricExpression, @QueryParam("inverseMetric") boolean inverseMetric,
      @QueryParam("cellSizeExpression") String cellSizeExpression, @QueryParam("rollupThreshold") Double rollupThreshold) {
    try {
      MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
      metricConfigDTO.setDataset(dataset);
      metricConfigDTO.setName(name);
      metricConfigDTO.setAlias(alias);
      metricConfigDTO.setDatatype(MetricType.valueOf(metricType));
      metricConfigDTO.setActive(active);

      // optional ones
      metricConfigDTO.setCellSizeExpression(cellSizeExpression);
      metricConfigDTO.setInverseMetric(inverseMetric);
      metricConfigDTO.setRollupThreshold(rollupThreshold);

      // handle derived
      if (derived) {
        if (derivedMetricExpression == null && numerator != null && denominator != null) {
          if ("RATIO".equals(derivedFunctionType)) {
            MetricConfigDTO numMetricConfigDTO = metricConfigDao.findByAliasAndDataset(numerator, dataset);
            MetricConfigDTO denMetricConfigDTO = metricConfigDao.findByAliasAndDataset(denominator, dataset);
            derivedMetricExpression = String.format("id%s/id%s", numMetricConfigDTO.getId(), denMetricConfigDTO.getId());
          }
          if ("PERCENT".equals(derivedFunctionType)) {
            MetricConfigDTO numMetricConfigDTO = metricConfigDao.findByAliasAndDataset(numerator, dataset);
            MetricConfigDTO denMetricConfigDTO = metricConfigDao.findByAliasAndDataset(denominator, dataset);
            derivedMetricExpression = String.format("id%s*100/id%s", numMetricConfigDTO.getId(), denMetricConfigDTO.getId());
          }
        }
        metricConfigDTO.setDerived(derived);
        metricConfigDTO.setDerivedMetricExpression(derivedMetricExpression);
      }
      Long id = metricConfigDao.save(metricConfigDTO);
      metricConfigDTO.setId(id);
      return JsonResponseUtil.buildResponseJSON(metricConfigDTO).toString();
    } catch (Exception e) {
      return JsonResponseUtil.buildErrorResponseJSON("Failed to create metric:" + name).toString();
    }
  }

  @GET
  @Path("/metrics")
  public String createMetricConfig(@NotNull @QueryParam("dataset") String dataset) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("dataset", dataset);
    List<MetricConfigDTO> metricConfigDTOs = metricConfigDao.findByParams(filters);
    List<String> metrics = new ArrayList<>();
    for (MetricConfigDTO metricConfigDTO : metricConfigDTOs) {
      metrics.add(metricConfigDTO.getAlias());
    }
    return JsonResponseUtil.buildResponseJSON(metrics).toString();
  }

  @GET
  @Path("/update")
  public String updateMetricConfig(@NotNull @QueryParam("id") long metricConfigId, @QueryParam("dataset") String dataset, @QueryParam("name") String name,
      @QueryParam("alias") String alias, @QueryParam("metricType") String metricType, @QueryParam("active") boolean active,
      @QueryParam("derived") boolean derived, @QueryParam("derivedMetricExpression") String derivedMetricExpression,
      @QueryParam("inverseMetric") boolean inverseMetric, @QueryParam("cellSizeExpression") String cellSizeExpression,
      @QueryParam("rollupThreshold") Double rollupThreshold) {
    try {

      MetricConfigDTO metricConfigDTO = metricConfigDao.findById(metricConfigId);
      metricConfigDTO.setDataset(dataset);
      metricConfigDTO.setName(name);
      metricConfigDTO.setAlias(alias);
      metricConfigDTO.setDatatype(MetricType.valueOf(metricType));
      metricConfigDTO.setActive(active);
      metricConfigDTO.setDerived(derived);
      metricConfigDTO.setDerivedMetricExpression(derivedMetricExpression);
      // optional ones
      metricConfigDTO.setCellSizeExpression(cellSizeExpression);
      metricConfigDTO.setInverseMetric(inverseMetric);
      metricConfigDTO.setRollupThreshold(rollupThreshold);
      int numRowsUpdated = metricConfigDao.update(metricConfigDTO);
      if (numRowsUpdated == 1) {
        return JsonResponseUtil.buildResponseJSON(metricConfigDTO).toString();
      } else {
        return JsonResponseUtil.buildErrorResponseJSON("Failed to update metric id:" + metricConfigId).toString();
      }
    } catch (Exception e) {
      return JsonResponseUtil.buildErrorResponseJSON("Failed to update metric id:" + metricConfigId + ". Exception:" + e.getMessage()).toString();
    }
  }

  @GET
  @Path("/delete")
  public String deleteMetricConfig(@NotNull @QueryParam("dataset") String dataset, @NotNull @QueryParam("id") Long metricConfigId) {
    metricConfigDao.deleteById(metricConfigId);
    return JsonResponseUtil.buildSuccessResponseJSON("Successully deleted " + metricConfigId).toString();
  }

  @GET
  @Path("/list")
  @Produces(MediaType.APPLICATION_JSON)
  public String viewMetricConfig(@NotNull @QueryParam("dataset") String dataset) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("dataset", dataset);
    List<MetricConfigDTO> metricConfigDTOs = metricConfigDao.findByParams(filters);
    ObjectNode rootNode = JsonResponseUtil.buildResponseJSON(metricConfigDTOs);
    return rootNode.toString();
  }

}
