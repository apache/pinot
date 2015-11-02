package com.linkedin.thirdeye.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.SplitSpec;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryLogBufferImpl;

public class StarTreeGeneratorStandalone {

  public static void main(String[] args) throws Exception {
	  
    Path path = new Path(args[0]);
    
    StarTreeConfig config = StarTreeConfig.decode(new FileInputStream(new File(args[1])));

    // set up star tree builder
    String collectionName = config.getCollection();
    List<String> splitOrder = config.getSplit().getOrder();
    MetricSchema metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());

    int maxRecordStoreEntries = config.getSplit().getThreshold();
    StarTreeConfig genConfig =
        new StarTreeConfig.Builder()
            .setRecordStoreFactoryClass(
                StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName())
            .setCollection(collectionName) //
            .setDimensions(config.getDimensions())//
            .setMetrics(config.getMetrics()).setTime(config.getTime()) //
            .setSplit(new SplitSpec(maxRecordStoreEntries, splitOrder)).setFixed(false).build();

    StarTree starTree = new StarTreeImpl(genConfig);
    starTree.open();

    
    SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), Reader.file(path));
    System.out.println(reader.getKeyClass());
    System.out.println(reader.getValueClassName());
    WritableComparable<?> key =(WritableComparable<?>) reader.getKeyClass().newInstance();
    Writable val =(Writable) reader.getValueClass().newInstance();
    MetricTimeSeries emptyTimeSeries = new MetricTimeSeries(metricSchema);

    while(reader.next(key,val)){
      BytesWritable writable = (BytesWritable) key;
      DimensionKey dimensionKey = DimensionKey.fromBytes(writable.getBytes());
      StarTreeRecord record = new StarTreeRecordImpl(config, dimensionKey, emptyTimeSeries);
      starTree.add(record);
      System.out.println(dimensionKey);
    }
  }
}
