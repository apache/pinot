package com.linkedin.thirdeye.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeBulkLoader;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestStarTreeBulkLoaderAvroImpl
{
  private File baseDir = new File(System.getProperty("java.io.tmpdir"),
                                  TestStarTreeBulkLoaderAvroImpl.class.getSimpleName());
  private File rootDir = new File(baseDir, "rootDir");
  private File tmpDir = new File(baseDir, "tmpDir");

  private UUID nodeId;
  private List<StarTreeRecord> records;
  private StarTreeRecordStore recordStore;
  private ExecutorService executorService;
  private StarTree starTree;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    FileUtils.forceMkdir(baseDir);
    FileUtils.forceMkdir(rootDir);
    FileUtils.forceMkdir(tmpDir);

    executorService = Executors.newSingleThreadExecutor();

    nodeId = UUID.randomUUID();

    records = new ArrayList<StarTreeRecord>();
    for (int i = 0; i < 100; i++)
    {
      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
      builder.setDimensionValue("A", "A" + (i % 2));
      builder.setDimensionValue("B", "B" + (i % 4));
      builder.setDimensionValue("C", "C" + (i % 8));
      builder.setMetricValue("M", 1);
      builder.setMetricType("M", "INT");
      builder.setTime((long) i);
      records.add(builder.build());
    }

    // Create a forward index
    int currentValueId = StarTreeConstants.FIRST_VALUE;
    Map<String, Map<String, Integer>> forwardIndex = new HashMap<String, Map<String, Integer>>();
    for (String dimensionName : Arrays.asList("A", "B", "C"))
    {
      forwardIndex.put(dimensionName, new HashMap<String, Integer>());
      for (int i = 0; i < 8; i++)
      {
        forwardIndex.get(dimensionName).put(dimensionName + i, currentValueId++);
      }
      forwardIndex.get(dimensionName).put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
      forwardIndex.get(dimensionName).put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
    }

    // Write store buffer
    OutputStream outputStream = new FileOutputStream(new File(rootDir, nodeId + ".buf"));
    StarTreeRecordStoreCircularBufferImpl.fillBuffer(
            outputStream, Arrays.asList("A", "B", "C"), Arrays.asList("M"),Arrays.asList("INT"), forwardIndex, records, 128, true);
    outputStream.flush();
    outputStream.close();

    // Write index
    outputStream = new FileOutputStream(new File(rootDir, nodeId + ".idx"));
    new ObjectMapper().writeValue(outputStream, forwardIndex);
    outputStream.flush();
    outputStream.close();

    TimeSpec timeSpec = new TimeSpec("hoursSinceEpoch",
                                     new TimeGranularity(1, TimeUnit.HOURS),
                                     new TimeGranularity(1, TimeUnit.HOURS),
                                     new TimeGranularity(128, TimeUnit.HOURS));

    // Create a tree with just that record store at root
    Properties recordStoreFactoryConfig = new Properties();
    StarTreeConfig config = new StarTreeConfig.Builder()
            .setCollection("myCollection")
            .setDimensionNames(Arrays.asList("A", "B", "C"))
            .setMetricNames(Arrays.asList("M"))
            .setMetricTypes(Arrays.asList("INT"))
            .setTime(timeSpec)
            .setRecordStoreFactoryConfig(recordStoreFactoryConfig)
            .setRecordStoreFactoryClass(StarTreeRecordStoreFactoryCircularBufferImpl.class.getCanonicalName())
            .build();
    starTree = new StarTreeImpl(config, rootDir, new StarTreeNodeImpl(
            nodeId,
            StarTreeConstants.STAR,
            StarTreeConstants.STAR,
            new ArrayList<String>(),
            new HashMap<String, String>(),
            new HashMap<String, StarTreeNode>(),
            null,
            null));
    starTree.open();

    // Create Avro data file to load into that store
    File bulkLoadDataDir = new File(tmpDir, "data");
    FileUtils.forceMkdir(bulkLoadDataDir);
    Schema schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream("MyRecord.avsc"));
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, new File(bulkLoadDataDir, nodeId + ".avro"));
    for (StarTreeRecord record : records)
    {
      dataFileWriter.append(StarTreeUtils.toGenericRecord(config, schema, record, null));
    }
    dataFileWriter.flush();
    dataFileWriter.close();
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    starTree.close();
    FileUtils.forceDelete(baseDir);
  }

  @Test
  public void testBulkLoad() throws Exception
  {
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "*")
            .setDimensionValue("C", "*")
            .build();

    StarTreeRecord r1 = starTree.getAggregate(query);
    Assert.assertEquals(r1.getMetricValues().get("M").intValue(), 100);

    StarTreeBulkLoader bulkLoader = new StarTreeBulkLoaderAvroImpl(executorService);
    bulkLoader.bulkLoad(starTree, rootDir, tmpDir);

    StarTreeRecord r2 = starTree.getAggregate(query);
    Assert.assertEquals(r2.getMetricValues().get("M").intValue(), 200);
  }
}
