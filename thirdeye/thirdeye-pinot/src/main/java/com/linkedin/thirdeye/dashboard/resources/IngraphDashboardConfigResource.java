package com.linkedin.thirdeye.dashboard.resources;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.IngraphDashboardConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.IngraphDashboardConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.util.JsonResponseUtil;

@Path(value = "/thirdeye-admin/ingraph-dashboard-config")
@Produces(MediaType.APPLICATION_JSON)
public class IngraphDashboardConfigResource {
  private static final Logger LOG = LoggerFactory.getLogger(IngraphDashboardConfigResource.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private IngraphDashboardConfigManager ingraphDashboardConfigDAO;
  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

  public IngraphDashboardConfigResource() {
    this.ingraphDashboardConfigDAO = DAO_REGISTRY.getIngraphDashboardConfigDAO();
  }

  @GET
  @Path("/create")
  public String createDashboardConfig(@QueryParam("name") String name, @QueryParam("fabricGroup") String fabricGroup,
      @QueryParam("active") boolean active,
      @QueryParam("bootstrap") boolean bootstrap, @QueryParam("fromIngraphDashboard") boolean fromIngraphDashboard,
      @QueryParam("startTime") String bootstrapStartTime, @QueryParam("endTime") String bootstrapEndTime,
      @QueryParam("fetchIntervalPeriod") String fetchIntervalPeriod, @QueryParam("mergeNumAvroRecords") String mergeNumAvroRecords,
      @QueryParam("granularitySize") String granularitySize, @QueryParam("granularityUnit") String granularityUnit) {
    try {
      IngraphDashboardConfigDTO ingraphDashboardConfigDTO = new IngraphDashboardConfigDTO();
      ingraphDashboardConfigDTO.setName(name);
      ingraphDashboardConfigDTO.setFabricGroup(fabricGroup);
      if (StringUtils.isNotBlank(fetchIntervalPeriod)) {
        ingraphDashboardConfigDTO.setFetchIntervalPeriod(Long.valueOf(fetchIntervalPeriod));
      }
      if (StringUtils.isNotBlank(mergeNumAvroRecords)) {
        ingraphDashboardConfigDTO.setMergeNumAvroRecords(Integer.valueOf(mergeNumAvroRecords));
      }
      if (StringUtils.isNotBlank(granularitySize)) {
        ingraphDashboardConfigDTO.setGranularitySize(Integer.valueOf(granularitySize));
      }
      if (StringUtils.isNotBlank(granularityUnit)) {
        ingraphDashboardConfigDTO.setGranularityUnit(TimeUnit.valueOf(granularityUnit));
      }

      Boolean isActive = Boolean.valueOf(active);
      ingraphDashboardConfigDTO.setActive(isActive);

      Boolean isFromIngraphDashboard = Boolean.valueOf(fromIngraphDashboard);
      ingraphDashboardConfigDTO.setFromIngraphDashboard(isFromIngraphDashboard);

      Boolean needBootstrap = Boolean.valueOf(bootstrap);
      ingraphDashboardConfigDTO.setBootstrap(needBootstrap);
      if (needBootstrap) {
        long startTimeInMs = sdf.parse(bootstrapStartTime).getTime();
        long endTimeInMs = sdf.parse(bootstrapEndTime).getTime();
        ingraphDashboardConfigDTO.setBootstrapStartTime(startTimeInMs);
        ingraphDashboardConfigDTO.setBootstrapEndTime(endTimeInMs);
      }
      Long id = ingraphDashboardConfigDAO.save(ingraphDashboardConfigDTO);
      ingraphDashboardConfigDTO.setId(id);
      return JsonResponseUtil.buildResponseJSON(ingraphDashboardConfigDTO).toString();
    } catch (Exception e) {
      LOG.error("Failed to create dashboard:{}", name, e);
      return JsonResponseUtil.buildErrorResponseJSON("Failed to create dashboard:" + name).toString();
    }
  }

  @GET
  @Path("/update")
  public String updateDashboardConfig(@NotNull @QueryParam("id") long ingraphDashboardConfigId,
      @QueryParam("name") String name,
      @QueryParam("fabricGroup") String fabricGroup,
      @QueryParam("active") boolean active,
      @QueryParam("bootstrap") boolean bootstrap , @QueryParam("fromIngraphDashboard") boolean fromIngraphDashboard,
      @QueryParam("startTime") String bootstrapStartTime,
      @QueryParam("endTime") String bootstrapEndTime, @QueryParam("fetchIntervalPeriod") String fetchIntervalPeriod,
      @QueryParam("mergeNumAvroRecords") String mergeNumAvroRecords, @QueryParam("granularitySize") String granularitySize,
      @QueryParam("granularityUnit") String granularityUnit) {
    try {

      IngraphDashboardConfigDTO ingraphDashboardConfigDTO = ingraphDashboardConfigDAO.findById(ingraphDashboardConfigId);
      ingraphDashboardConfigDTO.setName(name);
      ingraphDashboardConfigDTO.setFabricGroup(fabricGroup);
      if (StringUtils.isNotBlank(fetchIntervalPeriod)) {
        ingraphDashboardConfigDTO.setFetchIntervalPeriod(Long.valueOf(fetchIntervalPeriod));
      }
      if (StringUtils.isNotBlank(mergeNumAvroRecords)) {
        ingraphDashboardConfigDTO.setMergeNumAvroRecords(Integer.valueOf(mergeNumAvroRecords));
      }
      if (StringUtils.isNotBlank(granularitySize)) {
        ingraphDashboardConfigDTO.setGranularitySize(Integer.valueOf(granularitySize));
      }
      if (StringUtils.isNotBlank(granularityUnit)) {
        ingraphDashboardConfigDTO.setGranularityUnit(TimeUnit.valueOf(granularityUnit));
      }

      Boolean isActive = Boolean.valueOf(active);
      ingraphDashboardConfigDTO.setActive(isActive);

      Boolean isFromIngraphDashboard = Boolean.valueOf(fromIngraphDashboard);
      ingraphDashboardConfigDTO.setFromIngraphDashboard(isFromIngraphDashboard);

      Boolean needBootstrap = Boolean.valueOf(bootstrap);
      ingraphDashboardConfigDTO.setBootstrap(needBootstrap);
      if (needBootstrap) {
        long startTimeInMs = sdf.parse(bootstrapStartTime).getTime();
        long endTimeInMs = sdf.parse(bootstrapEndTime).getTime();
        ingraphDashboardConfigDTO.setBootstrapStartTime(startTimeInMs);
        ingraphDashboardConfigDTO.setBootstrapEndTime(endTimeInMs);
      }

      int numRowsUpdated = ingraphDashboardConfigDAO.update(ingraphDashboardConfigDTO);
      if (numRowsUpdated == 1) {
        return JsonResponseUtil.buildResponseJSON(ingraphDashboardConfigDTO).toString();
      } else {
        return JsonResponseUtil.buildErrorResponseJSON("Failed to update dashboard id:" + ingraphDashboardConfigId).toString();
      }
    } catch (Exception e) {
      return JsonResponseUtil.buildErrorResponseJSON("Failed to update dashboard id:" + ingraphDashboardConfigId + ". Exception:" + e.getMessage()).toString();
    }
  }

  @GET
  @Path("/delete")
  public String deleteDashboardConfig(@NotNull @QueryParam("dashboard") String dashboard, @NotNull @QueryParam("id") Long dashboardConfigId) {
    ingraphDashboardConfigDAO.deleteById(dashboardConfigId);
    return JsonResponseUtil.buildSuccessResponseJSON("Successully deleted " + dashboardConfigId).toString();
  }

  @GET
  @Path("/list")
  @Produces(MediaType.APPLICATION_JSON)
  public String viewDashboardConfigs(@DefaultValue("0") @QueryParam("jtStartIndex") int jtStartIndex,
      @DefaultValue("100") @QueryParam("jtPageSize") int jtPageSize) {
    List<IngraphDashboardConfigDTO> ingraphDashboardConfigDTOs = ingraphDashboardConfigDAO.findAll();
    List<IngraphDashboardConfigDTO> subList = Utils.sublist(ingraphDashboardConfigDTOs, jtStartIndex, jtPageSize);
    ObjectNode rootNode = JsonResponseUtil.buildResponseJSON(subList);
    return rootNode.toString();
  }

}
