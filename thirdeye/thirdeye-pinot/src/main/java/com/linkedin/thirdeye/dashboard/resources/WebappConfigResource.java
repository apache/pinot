package com.linkedin.thirdeye.dashboard.resources;

import java.io.IOException;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfig;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigClassFactory;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigClassFactory.WebappConfigType;
import com.linkedin.thirdeye.db.dao.WebappConfigDAO;
import com.linkedin.thirdeye.db.entity.WebappConfig;

@Path(value = "/dashboard")
@Produces(MediaType.APPLICATION_JSON)
public class WebappConfigResource {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(WebappConfigResource.class);
  private WebappConfigDAO webappConfigDAO;

  public WebappConfigResource(WebappConfigDAO webappConfigDAO) {
    this.webappConfigDAO = webappConfigDAO;
  }

  /**
   * @param anomalyResultId : anomaly result id
   * @param payload         : Json payload containing feedback @see com.linkedin.thirdeye.constant.AnomalyFeedbackType
   *                        eg. payload
   *                        <p/>
   *                        { "feedbackType": "NOT_ANOMALY", "comment": "this is not an anomaly" }
   */
  @POST
  @Path(value = "webapp-config/create/{collection}/{configType}")
  public Response createConfig(@PathParam("collection") String collection,
      @PathParam("configType") WebappConfigType configType,
      String payload) {


    try {
      AbstractConfig abstractConfig = AbstractConfig.fromJSON(payload,
          WebappConfigClassFactory.getClassNameFromConfigType(configType));
      String config = abstractConfig.toJSON();

      WebappConfig webappConfig = new WebappConfig();
      webappConfig.setCollection(collection);
      webappConfig.setConfigType(configType);
      webappConfig.setConfig(config);
      Long id = webappConfigDAO.save(webappConfig);
      LOG.info("Created webappConfig {} with id {}", webappConfig, id);
      return Response.ok(id).build();
    } catch (Exception e) {
      LOG.error("Invalid payload {} for configType {}", payload, configType, e);
      return Response.ok(e).build();
    }
  }

  public Response updateConfig(Long id, String collection, WebappConfigType configType, String payload) {
    WebappConfig webappConfig = webappConfigDAO.findById(id);
    webappConfig.setCollection(collection);
    webappConfig.setConfigType(configType);
    webappConfig.setConfig(payload);
    webappConfigDAO.update(webappConfig);
    return Response.ok(id).build();
  }
}
