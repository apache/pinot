package com.linkedin.thirdeye.impl.storage;

import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.UUID;

public class TestDataUpdateManager
{
  private File rootDir;
  private DataUpdateManager dataUpdateManager;
  private String collection;
  private String schedule;
  private String treeId;
  private DateTime minTime;
  private DateTime maxTime;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    rootDir = new File(System.getProperty("java.io.tmpdir"), TestDataUpdateManager.class.getCanonicalName());
    dataUpdateManager = new DataUpdateManager(rootDir);
    collection = "test";
    schedule = "TEST";
    treeId = UUID.randomUUID().toString();
    minTime = new DateTime(0);
    maxTime = new DateTime(1000);
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    FileUtils.forceDelete(rootDir);
  }

  @Test(enabled = false)
  public void testUpdateData() throws Exception
  {
    // TODO (requires using an actual data archive, so we need to generate that via some test framework)
  }

  @Test
  public void testDeleteData() throws Exception
  {
    // Create some collection data dir
    File collectionDir = new File(rootDir, collection);
    File dataDir = new File(collectionDir, StorageUtils.getDataDirName(treeId, schedule, minTime, maxTime));
    FileUtils.forceMkdir(dataDir);
    Assert.assertTrue(dataDir.exists());

    // Delete it
    dataUpdateManager.deleteData(collection, schedule, minTime, maxTime);
    Assert.assertFalse(dataDir.exists());
  }

  @Test
  public void testDeleteCollection() throws Exception
  {
    // Create some collection dir
    File collectionDir = new File(rootDir, collection);
    FileUtils.forceMkdir(collectionDir);
    Assert.assertTrue(collectionDir.exists());

    // Delete it
    dataUpdateManager.deleteCollection(collection);
    Assert.assertFalse(collectionDir.exists());
  }
}
