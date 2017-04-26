package com.linkedin.thirdeye.dashboard.resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.pojo.EntityToEntityMappingBean.MappingType;

@Path(value = "/entityMapping")
@Produces(MediaType.APPLICATION_JSON)
public class EntityMappingResource {

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private static final EntityToEntityMappingManager entityMappingDAO = DAO_REGISTRY.getEntityToEntityMappingDAO();


  public EntityMappingResource() {
  }

  @GET
  @Path("/view")
  @Produces(MediaType.APPLICATION_JSON)
  public List<EntityToEntityMappingDTO> viewEntityMappings(
      @QueryParam("fromUrn") String fromUrn,
      @QueryParam("toUrn") String toUrn,
      @QueryParam("mappingType") String mappingType) {

    List<EntityToEntityMappingDTO> mappings = new ArrayList<>();
    if (StringUtils.isBlank(fromUrn) && StringUtils.isBlank(toUrn) && StringUtils.isBlank(mappingType)) {
      mappings = entityMappingDAO.findAll();
    } else {
      Map<String, Object> filters = new HashMap<>();
      if (StringUtils.isNotBlank(fromUrn)) {
        filters.put("fromUrn", fromUrn);
      }
      if (StringUtils.isNotBlank(toUrn)) {
        filters.put("toUrn", toUrn);
      }
      if (StringUtils.isNotBlank(mappingType)) {
        filters.put("mappingType", mappingType);
      }
      mappings = entityMappingDAO.findByParams(filters);
    }
    return mappings;
  }

  @POST
  @Path("/create")
  public void createEntityMapping(String payload) {
    EntityToEntityMappingDTO entityToEntityMapping = null;
    try {
      entityToEntityMapping = OBJECT_MAPPER.readValue(payload, EntityToEntityMappingDTO.class);
      entityMappingDAO.save(entityToEntityMapping);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid payload " + payload, e);
    }
  }


  @POST
  @Path("/update/{id}")
  public void updateEntityMapping(
      @PathParam("id") Long id,
      @QueryParam("fromUrn") String fromUrn,
      @QueryParam("toUrn") String toUrn,
      @QueryParam("mappingType") String mappingType,
      @QueryParam("score") Double score) {

    EntityToEntityMappingDTO entityMappingDTO = entityMappingDAO.findById(id);
    if (entityMappingDTO != null) {
      if (StringUtils.isNotBlank(fromUrn)) {
        entityMappingDTO.setFromUrn(fromUrn);
      }
      if (StringUtils.isNotBlank(toUrn)) {
        entityMappingDTO.setToUrn(toUrn);
      }
      if (StringUtils.isNotBlank(mappingType)) {
        entityMappingDTO.setMappingType(MappingType.valueOf(mappingType));
      }
      if (score != null) {
        entityMappingDTO.setScore(score);
      }
      entityMappingDAO.update(entityMappingDTO);
    }
  }


  @DELETE
  @Path("/delete/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public void deleteEntityMappings(@PathParam("id") Long id) {
    EntityToEntityMappingDTO entityMappingDTO = entityMappingDAO.findById(id);
    if (entityMappingDTO != null) {
      entityMappingDAO.delete(entityMappingDTO);
    }
  }

}
