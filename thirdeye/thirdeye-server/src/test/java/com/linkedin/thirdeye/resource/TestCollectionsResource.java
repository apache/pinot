package com.linkedin.thirdeye.resource;

import java.io.File;
import java.io.FileInputStream;

import static org.mockito.Mockito.*;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.thirdeye.api.StarTreeManager;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public class TestCollectionsResource {

  MetricRegistry mockMetricRegistry;
  StarTreeManager mockStarTreeManager;
  String rootDirectory;
  String collection;
  File rootDir;

  CollectionsResource testCollectionsResource;

  public TestCollectionsResource() {

    mockMetricRegistry = mock(MetricRegistry.class);
    mockStarTreeManager = mock(StarTreeManager.class);
    rootDirectory = "/home/npawar/myprojects/pinot2_0/thirdeye/thirdeye-data/thirdeye-server";
    rootDir = new File(rootDirectory);

    collection = "abook";
    testCollectionsResource = new  CollectionsResource(mockStarTreeManager, mockMetricRegistry, rootDir);


  }


  public void testCollectionsResourcePostConfig(File newConfigFile)
  {
    byte[] configBytes = new byte[(int)newConfigFile.length()];
    FileInputStream configFileInputStream = null;
    try{
      configFileInputStream = new FileInputStream(newConfigFile);
      configFileInputStream.read(configBytes);
      configFileInputStream.close();

      Response postConfigResponse = testCollectionsResource.postConfig(collection, configBytes);
      System.out.println("Post Config Response : "+postConfigResponse.getStatus());
    }
    catch (WebApplicationException e)
    {
      System.out.println(e.getResponse().getStatus()+" "+e.getResponse().getEntity().toString());
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }


  public void testCollectionsResourcePostStartree(File newStarTreeFile)
  {
    byte[] starTreeBytes = new byte[(int)newStarTreeFile.length()];
    FileInputStream starTreeFileInputStream = null;
    try{
      starTreeFileInputStream = new FileInputStream(newStarTreeFile);
      starTreeFileInputStream.read(starTreeBytes);
      starTreeFileInputStream.close();

      Response postStarTreeResponse = testCollectionsResource.postStarTree(collection, starTreeBytes);
      System.out.println("Post Star Tree Response : "+postStarTreeResponse.getStatus());
    }
    catch (WebApplicationException e)
    {
      System.out.println(e.getResponse().getStatus()+" "+e.getResponse().getEntity().toString());
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public void testCollectionsResourcePostSchema(File newSchemaFile)
  {
    byte[] schemaBytes = new byte[(int)newSchemaFile.length()];
    FileInputStream schemaFileInputStream = null;
    try{
      schemaFileInputStream = new FileInputStream(newSchemaFile);
      schemaFileInputStream.read(schemaBytes);
      schemaFileInputStream.close();

      Response postSchemaResponse = testCollectionsResource.postSchema(collection, schemaBytes);
      System.out.println("Post Schema Response : "+postSchemaResponse.getStatus());
    }
    catch (WebApplicationException e)
    {
      System.out.println(e.getResponse().getStatus()+" "+e.getResponse().getEntity().toString());
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }


  public static void main(String[] args) {

    TestCollectionsResource testCollectionsResource = new TestCollectionsResource();

    String newConfigFile = "/home/npawar/myprojects/pinot2_0/thirdeye/thirdeye-data/thirdeye-server-ads/ads/config.yml";
    String newStarTreeFile = "/home/npawar/myprojects/pinot2_0/thirdeye/thirdeye-data/thirdeye-server-ads/ads/tree.bin";
    String newSchemaFile = "/home/npawar/myprojects/pinot2_0/thirdeye/thirdeye-data/thirdeye-server-ads/ads/schema.avsc";

    testCollectionsResource.testCollectionsResourcePostConfig(new File(newConfigFile));
    testCollectionsResource.testCollectionsResourcePostStartree(new File(newStarTreeFile));
    testCollectionsResource.testCollectionsResourcePostSchema(new File(newSchemaFile));

  }

}
