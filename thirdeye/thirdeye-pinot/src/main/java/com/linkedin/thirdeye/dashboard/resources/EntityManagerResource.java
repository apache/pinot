package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.WebappConfigDTO;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("thirdeye/entity")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class EntityManagerResource {
  private final WebappConfigManager webappConfigManager;
  private final AnomalyFunctionManager anomalyFunctionManager;
  private final EmailConfigurationManager emailConfigurationManager;

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(EntityManagerResource.class);

  public EntityManagerResource(WebappConfigManager webappConfigManager,
      AnomalyFunctionManager anomalyFunctionManager,
      EmailConfigurationManager emailConfigurationManager) {
    this.webappConfigManager = webappConfigManager;
    this.emailConfigurationManager = emailConfigurationManager;
    this.anomalyFunctionManager = anomalyFunctionManager;
  }

  private enum EntityType {
    WEBAPP_CONFIG, ANOMALY_FUNCTION, EMAIL_CONFIGURATION
  }

  @GET
  public List<EntityType> getAllEntityTypes() {
    return Arrays.asList(EntityType.values());
  }

  @GET
  @Path("{entityType}")
  public Response getAllEntitiesForType(@PathParam("entityType") String entityTypeStr) {
    EntityType entityType = EntityType.valueOf(entityTypeStr);
    List<AbstractDTO> results = new ArrayList<>();
    switch (entityType) {
    case WEBAPP_CONFIG:
      results.addAll(webappConfigManager.findAll());
      break;
    case ANOMALY_FUNCTION:
      results.addAll(anomalyFunctionManager.findAllActiveFunctions());
      break;
    case EMAIL_CONFIGURATION:
      results.addAll(emailConfigurationManager.findAll());
      break;
    default:
      throw new WebApplicationException("Unknown entity type : " + entityType);
    }
    return Response.ok(results).build();
  }

  @POST
  public Response updateEntity(@QueryParam("entityType") String entityTypeStr, String jsonPayload) {
    EntityType entityType = EntityType.valueOf(entityTypeStr);
    try {
      switch (entityType) {
      case WEBAPP_CONFIG:
        WebappConfigDTO webappConfigDTO =
            OBJECT_MAPPER.readValue(jsonPayload, WebappConfigDTO.class);
        webappConfigManager.update(webappConfigDTO);
        break;
      case ANOMALY_FUNCTION:
        AnomalyFunctionDTO anomalyFunctionDTO =
            OBJECT_MAPPER.readValue(jsonPayload, AnomalyFunctionDTO.class);
        anomalyFunctionManager.update(anomalyFunctionDTO);
        break;
      case EMAIL_CONFIGURATION:
        EmailConfigurationDTO emailConfigurationDTO =
            OBJECT_MAPPER.readValue(jsonPayload, EmailConfigurationDTO.class);
        emailConfigurationManager.update(emailConfigurationDTO);
        break;
      }
    } catch (IOException e) {
      LOG.error("Error saving the entity with payload : " + jsonPayload, e);
      throw new WebApplicationException(e);
    }
    return Response.ok().build();
  }
}

