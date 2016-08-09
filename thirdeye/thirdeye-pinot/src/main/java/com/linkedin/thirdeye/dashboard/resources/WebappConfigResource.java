package com.linkedin.thirdeye.dashboard.resources;

import javax.ws.rs.core.Response;

import com.linkedin.thirdeye.dashboard.configs.WebappConfigClassFactory.WebappConfigType;
import com.linkedin.thirdeye.db.dao.WebappConfigDAO;
import com.linkedin.thirdeye.db.entity.WebappConfig;

public class WebappConfigResource {

  private WebappConfigDAO webappConfigDAO;

  public WebappConfigResource(WebappConfigDAO webappConfigDAO) {
    this.webappConfigDAO = webappConfigDAO;
  }

  public Response createConfig(String collection, WebappConfigType configType, String payload) {
    WebappConfig webappConfig = new WebappConfig();
    webappConfig.setCollection(collection);
    webappConfig.setConfigType(configType);
    webappConfig.setConfig(payload);
    Long id = webappConfigDAO.save(webappConfig);
    return Response.ok(id).build();
  }
}
