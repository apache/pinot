package com.linkedin.thirdeye.dashboard.resources;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfig;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.dto.WebappConfigDTO;

@Path(value = "/webapp-config")
@Produces(MediaType.APPLICATION_JSON)
public class WebappConfigResource {

  private static final Logger LOG = LoggerFactory.getLogger(WebappConfigResource.class);
  private WebappConfigManager webappConfigDAO;

  public WebappConfigResource(WebappConfigManager webappConfigDAO) {
    this.webappConfigDAO = webappConfigDAO;
  }

  /**
   * @param collection
   * @param configType :DashboardConfig/CollectionSchema/CollectionConfig
   * @param payload    : Json payload containing AbstractConfig of configType
   *                     eg. payload
   *                     <p/>
   *                     { "collectionName" : "test_collection", "collectionAlias" : "test_alias" }"
   */
  @POST
  @Path(value = "create/{collection}/{type}")
  public Response createConfig(@PathParam("collection") String collection,
      @PathParam("type") WebappConfigType type,
      String payload) {

    try {
      AbstractConfig abstractConfig = WebappConfigFactory.getConfigFromConfigTypeAndJson(type, payload);

      String configName = abstractConfig.getConfigName();
      String config = abstractConfig.toJSON();

      WebappConfigDTO webappConfig = new WebappConfigDTO();
      webappConfig.setName(configName);
      webappConfig.setCollection(collection);
      webappConfig.setType(type);
      webappConfig.setConfig(config);
      Long id = webappConfigDAO.save(webappConfig);
      LOG.info("Created webappConfig {} with id {}", webappConfig, id);
      return Response.ok(id).build();
    } catch (Exception e) {
      LOG.error("Exception in creating webapp config with collection {} configType {} and payload {}",
          collection, type, payload, e);
      return Response.ok(e).build();
    }
  }

  @GET
  @Path(value = "view")
  public List<WebappConfigDTO> viewConfigs(@QueryParam("id") Long id, @QueryParam("collection") String collection,
      @QueryParam("type") WebappConfigType type) {
    List<WebappConfigDTO> webappConfigs = new ArrayList<>();
    if (id != null) {
      webappConfigs.add(webappConfigDAO.findById(id));
    } else if (!StringUtils.isBlank(collection)) {
      if (type != null) {
        webappConfigs.addAll(webappConfigDAO.findByCollectionAndType(collection, type));
      } else {
        webappConfigs.addAll(webappConfigDAO.findByCollection(collection));
      }
    } else if (type != null) {
      webappConfigs.addAll(webappConfigDAO.findByType(type));
    } else {
      webappConfigs.addAll(webappConfigDAO.findAll());
    }
    return webappConfigs;
  }

  @POST
  @Path(value = "update/{id}/{collection}/{configType}")
  public Response updateConfig(@PathParam("id") Long id, @PathParam("collection") String collection,
      @PathParam("type") WebappConfigType type, String payload) {
    try {
      AbstractConfig abstractConfig = WebappConfigFactory.getConfigFromConfigTypeAndJson(type, payload);

      String configName = abstractConfig.getConfigName();
      String config = abstractConfig.toJSON();

      WebappConfigDTO webappConfig = webappConfigDAO.findById(id);
      webappConfig.setName(configName);
      webappConfig.setCollection(collection);
      webappConfig.setType(type);
      webappConfig.setConfig(config);
      webappConfigDAO.update(webappConfig);
      return Response.ok(id).build();
    } catch (Exception e) {
      LOG.error("Exception in updating webapp config with id {} collection {} configType {} and payload {}",
          id, collection, type, payload, e);
      return Response.ok(e).build();
    }
  }

  @DELETE
  @Path(value = "delete")
  public Response deleteConfig(@QueryParam("id") Long id, @QueryParam("collection") String collection,
      @QueryParam("type") WebappConfigType type) {
    try {
      if (id == null && StringUtils.isBlank(collection)) {
        throw new IllegalStateException("must specify id or collection");
      }
      List<WebappConfigDTO> webappConfigs;
      if (id != null) {
        webappConfigs = new ArrayList<>();
        webappConfigs.add(webappConfigDAO.findById(id));
      } else if (type == null) {
        webappConfigs = webappConfigDAO.findByCollection(collection);
      } else {
        webappConfigs = webappConfigDAO.findByCollectionAndType(collection, type);
      }
      for (WebappConfigDTO webappConfig : webappConfigs) {
        webappConfigDAO.deleteById(webappConfig.getId());
      }
      return Response.ok(id).build();
    } catch (Exception e) {
      return Response.ok(e).build();
    }
  }
}
