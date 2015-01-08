package com.linkedin.thirdeye.tools;

import java.io.File;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.api.SplitSpec;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationConfig;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;

public class StarTreeGenTool {
  private static final Logger LOG = LoggerFactory
      .getLogger(StarTreeGenTool.class);

  public static void main(String[] args) throws Exception {

    Path inputPath = new Path(args[0]);
    Path configPath = new Path(args[1]);
    String outputDir = args[2];
    Path outputPath = new Path(outputDir);

    SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(),
        Reader.file(inputPath));
    System.out.println(reader.getKeyClass());
    System.out.println(reader.getValueClassName());
    WritableComparable<?> key = (WritableComparable<?>) reader.getKeyClass()
        .newInstance();
    Writable val = (Writable) reader.getValueClass().newInstance();

    FileSystem fs = FileSystem.get(new Configuration());
    StarTreeGenerationConfig config = new ObjectMapper().readValue(
        fs.open(configPath), StarTreeGenerationConfig.class);

    String collectionName = config.getCollectionName();
    String timeColumnName = config.getTimeColumnName();
    List<String> splitOrder = config.getSplitOrder();
    int maxRecordStoreEntries = config.getSplitThreshold();
    List<String> dimensionNames = config.getDimensionNames();
    List<String> metricNames = config.getMetricNames();
    String recordStoreFactoryClass = "com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryCircularBufferImpl";
    Properties recordStoreFactoryConfig = new Properties();
    recordStoreFactoryConfig.setProperty("numTimeBuckets", "672");
    System.out.println(outputPath.toUri().toString());
    recordStoreFactoryConfig.setProperty("rootDir", "");

    TimeSpec timeSpec = new TimeSpec(config.getTimeColumnName(),
                                     new TimeGranularity(1, TimeUnit.HOURS),
                                     new TimeGranularity(1, TimeUnit.HOURS),
                                     new TimeGranularity(672, TimeUnit.HOURS));

    StarTreeConfig starTreeConfig = new StarTreeConfig.Builder()
        .setCollection(collectionName) //
        .setDimensionNames(dimensionNames)//
        .setMetricNames(metricNames)//
        .setTime(timeSpec) //
        .setSplit(new SplitSpec(maxRecordStoreEntries, splitOrder)).build();
    System.out.println(starTreeConfig.encode());
    int rowCount = 0;
    StarTree starTree = new StarTreeImpl(starTreeConfig);
    Map<String, String> metricTypesMap = Collections.emptyMap();
    Map<String, Number> metricValuesMap = Collections.emptyMap();

    while (reader.next(key, val)) {
      BytesWritable writable = (BytesWritable) key;
      DimensionKey dimensionKey = DimensionKey.fromBytes(writable.getBytes());
      System.out.println(dimensionKey);
      Map<String, String> dimensionValuesMap = new HashMap<String, String>();
      for (int i = 0; i < dimensionNames.size(); i++) {
        dimensionValuesMap.put(dimensionNames.get(i),
            dimensionKey.getDimensionsValues()[i]);
      }
      
      Long time = 0l;
      StarTreeRecord record = new StarTreeRecordImpl(dimensionValuesMap,
          metricValuesMap, metricTypesMap, time);
      starTree.add(record);
      rowCount = rowCount + 1;
    }
    System.out.println("Number of records added:" + rowCount);
    // dump the star tree
    PrintWriter printWriter = new PrintWriter(System.out);

    StarTreeNode root = starTree.getRoot();
    StarTreeDumperTool tool = new StarTreeDumperTool(root, printWriter);
    tool.print();
    
    //store the tree
    System.out.println("Saving tree at "+ outputDir);
    StarTreePersistanceUtil.saveTree(starTree, outputDir);
    new File(outputDir + "/data").mkdirs();
    System.out.println("Saving leaf data at "+ outputDir + "/data");
    StarTreePersistanceUtil.saveLeafDimensionData(starTree, outputDir +"/data");
  }

}
