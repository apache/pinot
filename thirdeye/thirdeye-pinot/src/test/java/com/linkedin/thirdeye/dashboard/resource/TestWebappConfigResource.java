package com.linkedin.thirdeye.dashboard.resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.dashboard.configs.AbstractConfig;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigClassFactory.WebappConfigType;
import com.linkedin.thirdeye.dashboard.resources.WebappConfigResource;
import com.linkedin.thirdeye.db.entity.WebappConfig;

public class TestWebappConfigResource extends AbstractDbTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestWebappConfigResource.class);

  private String collection = "test_collection";
  private String alias = "test_alias";
  private WebappConfigType configType = WebappConfigType.CollectionConfig;
  String payload = "{ \"collectionName\" : \"test_collection\", \"collectionAlias\" : \"test_alias\" }";

  WebappConfigResource webappConfigResource;

  @Test
  public void testCreateConfig() throws Exception {
    webappConfigResource = new WebappConfigResource(webappConfigDAO);

    webappConfigResource.createConfig(collection, configType, payload);

    List<WebappConfig> webappConfigs = webappConfigDAO.findByCollection(collection);
    Assert.assertEquals(webappConfigs.size(), 1);
    Assert.assertEquals(webappConfigs.get(0).getCollection(), collection);
    Assert.assertEquals(webappConfigs.get(0).getConfigType(), configType);
    CollectionConfig expectedCollectionConfig = CollectionConfig.fromJSON(payload, CollectionConfig.class);
    Assert.assertEquals(webappConfigs.get(0).getConfig(), expectedCollectionConfig.toJSON());
  }

}
