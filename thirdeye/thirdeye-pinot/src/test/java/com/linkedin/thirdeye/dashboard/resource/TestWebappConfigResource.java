package com.linkedin.thirdeye.dashboard.resource;


import java.util.List;

import javax.ws.rs.core.Response;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;
import com.linkedin.thirdeye.dashboard.resources.WebappConfigResource;
import com.linkedin.thirdeye.datalayer.dto.WebappConfigDTO;
import com.linkedin.thirdeye.db.dao.AbstractDbTestBase;

public class TestWebappConfigResource extends AbstractDbTestBase {

  private String collection = "test_collection";
  private WebappConfigType type = WebappConfigType.COLLECTION_CONFIG;
  private String payload = "{ \"collectionName\" : \"test_collection\", \"collectionAlias\" : \"test_alias\" }";
  private Long id;

  WebappConfigResource webappConfigResource;

  @Test
  public void testCreateConfig() throws Exception {
    webappConfigResource = new WebappConfigResource(webappConfigDAO);

    Response r = webappConfigResource.createConfig(collection, type, payload);
    id = (Long) r.getEntity();

    WebappConfigDTO webappConfig = webappConfigDAO.findById(id);
    Assert.assertEquals(webappConfig.getId(), id);
    Assert.assertEquals(webappConfig.getCollection(), collection);
    Assert.assertEquals(webappConfig.getType(), type);
    CollectionConfig expectedCollectionConfig = CollectionConfig.fromJSON(payload, CollectionConfig.class);
    Assert.assertEquals(webappConfig.getConfig(), expectedCollectionConfig.toJSON());
  }

  @Test(dependsOnMethods = {"testCreateConfig"})
  public void testViewConfigs() throws Exception {
    List<WebappConfigDTO> webappConfigs = webappConfigResource.viewConfigs(id, null, null);
    Assert.assertEquals(webappConfigs.size(), 1);
    webappConfigs = webappConfigResource.viewConfigs(null, null, null);
    Assert.assertEquals(webappConfigs.size(), 1);
    webappConfigs = webappConfigResource.viewConfigs(null, "dummy", null);
    Assert.assertEquals(webappConfigs.size(), 0);
    webappConfigs = webappConfigResource.viewConfigs(null, collection, null);
    Assert.assertEquals(webappConfigs.size(), 1);
    webappConfigs = webappConfigResource.viewConfigs(null, null, type);
    Assert.assertEquals(webappConfigs.size(), 1);
    webappConfigs = webappConfigResource.viewConfigs(null, null, WebappConfigType.COLLECTION_SCHEMA);
    Assert.assertEquals(webappConfigs.size(), 0);
  }

  @Test(dependsOnMethods = {"testViewConfigs"})
  public void testUpdateConfig() throws Exception {
    String updatedCollection = "update_collection";
    String updatedPayload = "{ \"collectionName\" : \"update_collection\", \"collectionAlias\" : \"test_alias\" }";
    webappConfigResource.updateConfig(id, updatedCollection, type, updatedPayload);

    WebappConfigDTO webappConfig = webappConfigDAO.findById(id);
    Assert.assertEquals(webappConfig.getCollection(), updatedCollection);
    Assert.assertEquals(webappConfig.getType(), type);
    CollectionConfig expectedCollectionConfig = CollectionConfig.fromJSON(updatedPayload, CollectionConfig.class);
    Assert.assertEquals(webappConfig.getConfig(), expectedCollectionConfig.toJSON());
  }

  @Test(dependsOnMethods = {"testUpdateConfig"})
  public void testDeleteConfig() throws Exception {
    webappConfigResource.deleteConfig(id, null, null);
    Assert.assertNull(webappConfigDAO.findById(id));

    webappConfigResource.createConfig(collection, type, payload);
    Assert.assertEquals(webappConfigDAO.findAll().size(), 1);

    webappConfigResource.deleteConfig(null, collection, WebappConfigType.COLLECTION_SCHEMA);
    Assert.assertEquals(webappConfigDAO.findAll().size(), 1);

    webappConfigResource.deleteConfig(null, collection, null);
    Assert.assertEquals(webappConfigDAO.findAll().size(), 0);
  }

}
