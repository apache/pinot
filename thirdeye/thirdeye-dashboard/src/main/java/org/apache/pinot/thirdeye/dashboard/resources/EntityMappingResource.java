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

package org.apache.pinot.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Singleton;
import org.apache.pinot.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import org.apache.pinot.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;

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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class EntityMappingResource {

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final EntityToEntityMappingManager entityMappingDAO = DAO_REGISTRY.getEntityToEntityMappingDAO();
  private static final Logger LOG = LoggerFactory.getLogger(EntityMappingResource.class);

  public EntityMappingResource() {
  }

  @GET
  @Path("/view")
  @Produces(MediaType.APPLICATION_JSON)
  public List<EntityToEntityMappingDTO> viewEntityMappings(
      @QueryParam("fromURN") String fromURN,
      @QueryParam("toURN") String toURN,
      @QueryParam("mappingType") String mappingType) {

    List<EntityToEntityMappingDTO> mappings = new ArrayList<>();
    if (StringUtils.isBlank(fromURN) && StringUtils.isBlank(toURN) && mappingType == null) {
      mappings = entityMappingDAO.findAll();
    } else {
      Map<String, Object> filters = new HashMap<>();
      if (StringUtils.isNotBlank(fromURN)) {
        filters.put("fromURN", fromURN);
      }
      if (StringUtils.isNotBlank(toURN)) {
        filters.put("toURN", toURN);
      }
      if (mappingType != null) {
        filters.put("mappingType", mappingType);
      }
      mappings = entityMappingDAO.findByParams(filters);
    }
    return mappings;
  }


  @GET
  @Path("/view/all")
  @Produces(MediaType.APPLICATION_JSON)
  public List<EntityToEntityMappingDTO> viewAllEntityMappings() {
    List<EntityToEntityMappingDTO> mappings = entityMappingDAO.findAll();
    return mappings;
  }

  @GET
  @Path("/view/fromURN/{fromURN}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<EntityToEntityMappingDTO> viewEntityMappingsFromURN(@PathParam("fromURN") String fromURN) {

    List<EntityToEntityMappingDTO> mappings = entityMappingDAO.findByFromURN(fromURN);
    return mappings;
  }

  @GET
  @Path("/view/toURN/{toURN}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<EntityToEntityMappingDTO> viewEntityMappingsToURN(@PathParam("toURN") String toURN) {

    List<EntityToEntityMappingDTO> mappings = entityMappingDAO.findByToURN(toURN);
    return mappings;
  }

  @GET
  @Path("/view/mappingType/{mappingType}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<EntityToEntityMappingDTO> viewEntityMappingsForMappingType(
      @PathParam("mappingType") String mappingType) {

    List<EntityToEntityMappingDTO> mappings = entityMappingDAO.findByMappingType(mappingType);
    return mappings;
  }


  @GET
  @Path("/view/fromURN/{fromURN}/toURN/{toURN}")
  @Produces(MediaType.APPLICATION_JSON)
  public EntityToEntityMappingDTO viewEntityMappingsFromAndToURN(
      @PathParam("fromURN") String fromURN,
      @PathParam("toURN") String toURN) {

    EntityToEntityMappingDTO mapping = entityMappingDAO.findByFromAndToURN(fromURN, toURN);
    return mapping;
  }


  @GET
  @Path("/view/fromURN/{fromURN}/mappingType/{mappingType}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<EntityToEntityMappingDTO> viewEntityMappingsFromURNAndMappingType(
      @PathParam("fromURN") String fromURN,
      @PathParam("mappingType") String mappingType) {

    List<EntityToEntityMappingDTO> mappings = entityMappingDAO.findByFromURNAndMappingType(fromURN, mappingType);
    return mappings;
  }


  @GET
  @Path("/view/toURN/{toURN}/mappingType/{mappingType}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<EntityToEntityMappingDTO> viewEntityMappingsToURNAndMappingType(
      @PathParam("toURN") String toURN,
      @PathParam("mappingType") String mappingType) {

    List<EntityToEntityMappingDTO> mappings = entityMappingDAO.findByToURNAndMappingType(toURN, mappingType);
    return mappings;
  }

  /**
   * Create this by providing json payload as follows:
   *
   *    curl -H "Content-Type: application/json" -X POST -d <payload> <url>
   *    Eg: curl -H "Content-Type: application/json" -X POST -d
   *            '{"fromURN":"xyz","toURN":"xyz", "mappingType":"METRIC_TO_METRIC", "score":1.0}'
   *                http://localhost:8080/entityMapping/create
   * @param payload
   */
  @POST
  @Path("/create")
  public Response createEntityMapping(String payload) {
    EntityToEntityMappingDTO entityToEntityMapping = null;
    Response response = null;
    try {
      entityToEntityMapping = OBJECT_MAPPER.readValue(payload, EntityToEntityMappingDTO.class);
      EntityToEntityMappingDTO existingMapping = entityMappingDAO
          .findByFromAndToURN(entityToEntityMapping.getFromURN(), entityToEntityMapping.getToURN());
      Long id = null;
      if (existingMapping != null) {
        entityToEntityMapping.setId(id);
        entityMappingDAO.update(entityToEntityMapping);
      } else {
        id = entityMappingDAO.save(entityToEntityMapping);
      }
      response = Response.status(Status.OK).entity(String.format("Created mapping with id %d", id))
          .build();
    } catch (Exception e) {
      response = Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(String.format("Invalid payload %s %s", payload, e)).build();
      LOG.error(e.getMessage(), e);
    }
    return response;
  }

  /**
   * Update the entity mapping by providing the changes in query params
   * @param id
   * @param fromURN
   * @param toURN
   * @param mappingType
   * @param score
   * @return
   */
  @POST
  @Path("/update/{id}")
  public Response updateEntityMapping(
      @PathParam("id") Long id,
      @QueryParam("fromURN") String fromURN,
      @QueryParam("toURN") String toURN,
      @QueryParam("mappingType") String mappingType,
      @QueryParam("score") Double score) {

    Response response = Response.status(Status.NOT_FOUND).build();
    EntityToEntityMappingDTO entityMappingDTO = entityMappingDAO.findById(id);
    if (entityMappingDTO != null) {
      if (StringUtils.isNotBlank(fromURN)) {
        entityMappingDTO.setFromURN(fromURN);
      }
      if (StringUtils.isNotBlank(toURN)) {
        entityMappingDTO.setToURN(toURN);
      }
      if (mappingType != null) {
        entityMappingDTO.setMappingType(mappingType);
      }
      if (score != null) {
        entityMappingDTO.setScore(score);
      }
      entityMappingDAO.update(entityMappingDTO);
      response = Response.ok().build();
    }
    return response;
  }


  @DELETE
  @Path("/delete/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteEntityMappings(@PathParam("id") Long id) {
    Response response = Response.status(Status.NOT_FOUND).build();
    EntityToEntityMappingDTO entityMappingDTO = entityMappingDAO.findById(id);
    if (entityMappingDTO != null) {
      entityMappingDAO.delete(entityMappingDTO);
      response = Response.ok().build();
    }
    return response;
  }
}
