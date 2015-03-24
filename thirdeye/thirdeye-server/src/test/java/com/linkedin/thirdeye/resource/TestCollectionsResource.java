package com.linkedin.thirdeye.resource;

import java.io.File;

import static org.mockito.Mockito.*;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.sun.jersey.api.ConflictException;

import javax.ws.rs.core.Response;

import org.apache.commons.io.FileUtils;
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

    testCollectionsResource = new  CollectionsResource(mockStarTreeManager, mockMetricRegistry, rootDir);

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

  @Test
  public void testPostStarTree() throws Exception
  {
    byte[] starTreeBytes = "Dummy star tree file".getBytes();
    Response postStarTreeResponse = testCollectionsResource.postStarTree(collection, starTreeBytes);
    Assert.assertEquals(postStarTreeResponse.getStatus(), Response.Status.OK.getStatusCode());
  }


  @Test(expectedExceptions = ConflictException.class)
  public void testPostStarTreeOverwrite() throws Exception
  {
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.exists())
    {
      FileUtils.forceMkdir(collectionDir);
    }

    File starTreeFile = new File(collectionDir, StarTreeConstants.TREE_FILE_NAME);

    FileUtils.writeByteArrayToFile(starTreeFile, "Dummy existing star tree file".getBytes());

    byte[] starTreeBytes = "Dummy star tree file to overwrite".getBytes();
    Response postStarTreeResponse = testCollectionsResource.postStarTree(collection, starTreeBytes);

  }

  @Test
  public void testPostSchema() throws Exception
  {
    byte[] schemaBytes = "Dummy schema file".getBytes();
    Response postSchemaResponse = testCollectionsResource.postSchema(collection, schemaBytes);
    Assert.assertEquals(postSchemaResponse.getStatus(), Response.Status.OK.getStatusCode());
  }


  @Test(expectedExceptions = ConflictException.class)
  public void testPostSchemaOverwrite() throws Exception
  {
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.exists())
    {
      FileUtils.forceMkdir(collectionDir);
    }

    File schemaFile = new File(collectionDir, StarTreeConstants.SCHEMA_FILE_NAME);

    FileUtils.writeByteArrayToFile(schemaFile, "Dummy existing schema file".getBytes());

    byte[] schemaBytes = "Dummy schema file to overwrite".getBytes();
    Response postConfigResponse = testCollectionsResource.postSchema(collection, schemaBytes);

  }


}
