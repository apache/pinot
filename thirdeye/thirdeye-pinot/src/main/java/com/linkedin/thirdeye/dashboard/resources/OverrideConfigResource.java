package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/dashboard")
@Produces(MediaType.APPLICATION_JSON)
public class OverrideConfigResource {

  private static final Logger LOG = LoggerFactory.getLogger(OverrideConfigResource.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @GET
  @Path("/override-config/view")
  public List<OverrideConfigDTO> viewOverrideConfig (
      @NotNull @QueryParam("startTime") long startTimeMillis,
      @NotNull @QueryParam("endTime") long endTimeMillis,
      @QueryParam("targetEntity") String targetEntity) {
    OverrideConfigManager overrideConfigDAO = DAO_REGISTRY.getOverrideConfigDAO();
    if (StringUtils.isEmpty(targetEntity)) {
      return overrideConfigDAO.findAllConflict(startTimeMillis, endTimeMillis);
    } else {
      return overrideConfigDAO
          .findAllConflictByTargetType(targetEntity, startTimeMillis, endTimeMillis);
    }
  }

  @POST
  @Path("/override-config/create")
  public Response createOverrideConfig(
      @NotNull @QueryParam("startTime") long startTimeMillis,
      @NotNull @QueryParam("endTime") long endTimeMillis,
      @QueryParam("targetLevel") String targetLevelJson,
      @NotNull @QueryParam("targetEntity") String targetEntity,
      @QueryParam("overrideProperties") String overridePropertiesJson,
      @QueryParam("active") boolean active) {

    Map<String, List<String>> targetLevel;
    if (StringUtils.isEmpty(targetLevelJson)) {
      targetLevel = Collections.emptyMap();
    } else {
      try {
        targetLevel = OBJECT_MAPPER.readValue(targetLevelJson, HashMap.class);
      } catch (IOException e) {
        LOG.error("Invalid JSON string {}", targetLevelJson);
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
    }

    if (StringUtils.isEmpty(targetEntity)) {
      LOG.error("Received null for one of the mandatory params \"targetEntity\": {}", targetEntity);
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    }

    Map<String, String> overrideProperties;
    if (StringUtils.isEmpty(overridePropertiesJson)) {
      overrideProperties = Collections.emptyMap();
    } else {
      try {
        overrideProperties = OBJECT_MAPPER.readValue(overridePropertiesJson, HashMap.class);
      } catch (IOException e) {
        LOG.error("Invalid JSON string {}", overridePropertiesJson);
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
    }

    OverrideConfigDTO overrideConfigDTO = new OverrideConfigDTO();
    overrideConfigDTO.setStartTime(startTimeMillis);
    overrideConfigDTO.setEndTime(endTimeMillis);
    overrideConfigDTO.setTargetLevel(targetLevel);
    overrideConfigDTO.setTargetEntity(targetEntity);
    overrideConfigDTO.setOverrideProperties(overrideProperties);
    overrideConfigDTO.setActive(active);

    OverrideConfigManager overrideConfigDAO = DAO_REGISTRY.getOverrideConfigDAO();

    // Check if there exists any duplicate override config
    List<OverrideConfigDTO> existingOverrideConfigDTOs =
        overrideConfigDAO.findAllConflictByTargetType(targetEntity, startTimeMillis, endTimeMillis);
    for (OverrideConfigDTO existingOverrideConfig : existingOverrideConfigDTOs) {
      if (existingOverrideConfig.equals(overrideConfigDTO)) {
        LOG.error("Exists a duplicate override config: {}", existingOverrideConfig.toString());
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
    }

    overrideConfigDAO.save(overrideConfigDTO);
    return Response.ok().build();
  }

  @POST
  @Path("/override-config/update")
  public Response updateOverrideConfig(
      @NotNull @QueryParam("id") long id,
      @NotNull @QueryParam("startTime") long startTimeMillis,
      @NotNull @QueryParam("endTime") long endTimeMillis,
      @QueryParam("targetLevel") String targetLevelJson,
      @NotNull @QueryParam("targetEntity") String targetEntity,
      @QueryParam("overrideProperties") String overridePropertiesJson,
      @QueryParam("active") boolean active) {

    Map<String, List<String>> targetLevel;
    if (StringUtils.isEmpty(targetLevelJson)) {
      targetLevel = Collections.emptyMap();
    } else {
      try {
        targetLevel = OBJECT_MAPPER.readValue(targetLevelJson, HashMap.class);
      } catch (IOException e) {
        LOG.error("Invalid JSON string {}", targetLevelJson);
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
    }

    if (StringUtils.isEmpty(targetEntity)) {
      LOG.error("Received null for one of the mandatory params \"targetEntity\": {}", targetEntity);
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    }

    Map<String, String> overrideProperties;
    if (StringUtils.isEmpty(overridePropertiesJson)) {
      overrideProperties = Collections.emptyMap();
    } else {
      try {
        overrideProperties = OBJECT_MAPPER.readValue(overridePropertiesJson, HashMap.class);
      } catch (IOException e) {
        LOG.error("Invalid JSON string {}", overridePropertiesJson);
        return Response.status(Response.Status.NOT_ACCEPTABLE).build();
      }
    }

    OverrideConfigManager overrideConfigDAO = DAO_REGISTRY.getOverrideConfigDAO();

    OverrideConfigDTO overrideConfigDTO = overrideConfigDAO.findById(id);
    if (overrideConfigDTO == null) {
      LOG.error("Unable to find override config with id {}", id);
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    }

    overrideConfigDTO.setStartTime(startTimeMillis);
    overrideConfigDTO.setEndTime(endTimeMillis);
    overrideConfigDTO.setTargetLevel(targetLevel);
    overrideConfigDTO.setTargetEntity(targetEntity);
    overrideConfigDTO.setOverrideProperties(overrideProperties);
    overrideConfigDTO.setActive(active);

    overrideConfigDAO.update(overrideConfigDTO);
    return Response.ok().build();
  }

  @DELETE
  @Path("/override-config/delete")
  public Response deleteOverrideConfig(@NotNull @QueryParam("id") long id) {
    OverrideConfigManager overrideConfigDAO = DAO_REGISTRY.getOverrideConfigDAO();
    overrideConfigDAO.deleteById(id);
    return Response.ok().build();
  }
}
