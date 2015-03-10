package com.linkedin.thirdeye.tools;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.Reader;

import com.linkedin.thirdeye.api.DimensionKey;

public class AggregationOutputDumpTool {

  public static void main(String[] args) throws Exception {
    Properties properties = System.getProperties();
    Configuration configuration = new Configuration();
    configuration.set("fs.default.name", "");

    Object val = properties.get("fs.defaultFS");
    if(val!=null){
      configuration.set("fs.default.name", val.toString());
    }

    Path path = new Path(args[0]);
    FileSystem fs = FileSystem.get(configuration);
    if (fs.isDirectory(path)) {
      FileStatus[] listStatus = fs.listStatus(path);
      for (FileStatus fileStatus : listStatus) {
        processFile(fileStatus.getPath());
      }
    } else {
      processFile(path);

    }
  }

  private static void processFile(Path path) throws Exception {
    System.out.println("Processing file:" + path);
    SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(),
        Reader.file(path));
    System.out.println(reader.getKeyClass());
    System.out.println(reader.getValueClassName());
    WritableComparable<?> key = (WritableComparable<?>) reader.getKeyClass()
        .newInstance();
    Writable val = (Writable) reader.getValueClass().newInstance();

    while (reader.next(key, val)) {
      BytesWritable writable = (BytesWritable) key;
      DimensionKey dimensionKey = DimensionKey.fromBytes(writable.getBytes());
      System.out.println(dimensionKey);
    }
  }
}
