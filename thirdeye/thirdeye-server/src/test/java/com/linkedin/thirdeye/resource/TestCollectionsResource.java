package com.linkedin.thirdeye.resource;

import java.io.File;
import java.util.UUID;

import static org.mockito.Mockito.*;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.storage.DataUpdateManager;
import com.linkedin.thirdeye.impl.storage.StorageUtils;
import com.sun.jersey.api.ConflictException;

import javax.ws.rs.core.Response;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCollectionsResource {

  private MetricRegistry mockMetricRegistry;
  private StarTreeManager mockStarTreeManager;
  private String collection;
  private File rootDir;

  CollectionsResource testCollectionsResource;

  @BeforeMethod
  public void beforeMethod() throws Exception
  {
    mockMetricRegistry = mock(MetricRegistry.class);
    mockStarTreeManager = mock(StarTreeManager.class);
    rootDir = new File(System.getProperty("java.io.tmpdir"), TestCollectionsResource.class.getName());

    try { FileUtils.forceDelete(rootDir); } catch (Exception e) { /* ok */ }
    try { FileUtils.forceMkdir(rootDir); } catch (Exception e) { /* ok */ }

    testCollectionsResource = new  CollectionsResource(mockStarTreeManager, mockMetricRegistry, new DataUpdateManager(rootDir), rootDir);

    collection = "dummy";
  }

  @AfterMethod
  public void afterMethod() throws Exception
  {
    try { FileUtils.forceDelete(rootDir); } catch (Exception e) { /* ok */ }
  }

  @Test
  public void testPostConfig() throws Exception
  {
    byte[] configBytes = "Dummy config file".getBytes();
    Response postConfigResponse = testCollectionsResource.postConfig(collection, configBytes);
    Assert.assertEquals(postConfigResponse.getStatus(), Response.Status.OK.getStatusCode());
  }


  @Test(expectedExceptions = ConflictException.class)
  public void testPostConfigOverwrite() throws Exception
  {
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.exists())
    {
      FileUtils.forceMkdir(collectionDir);
    }

    File configFile = new File(collectionDir, StarTreeConstants.CONFIG_FILE_NAME);

    FileUtils.writeByteArrayToFile(configFile, "Dummy existing config file".getBytes());

    byte[] configBytes = "Dummy config file to overwrite".getBytes();
    Response postConfigResponse = testCollectionsResource.postConfig(collection, configBytes);

  }
}
