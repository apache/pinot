package com.linkedin.thirdeye.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.linkedin.thirdeye.api.DimensionKey;

public class RollupOutputDumpTool {

  public static void main(String[] args)
      throws IOException, InstantiationException, IllegalAccessException {
    Path path = new Path(args[0]);
    SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), Reader.file(path));
    System.out.println(reader.getKeyClass());
    System.out.println(reader.getValueClassName());
    WritableComparable<?> key = (WritableComparable<?>) reader.getKeyClass().newInstance();
    Writable val = (Writable) reader.getValueClass().newInstance();

    while (reader.next(key, val)) {
      BytesWritable writable = (BytesWritable) key;
      DimensionKey dimensionKey = DimensionKey.fromBytes(writable.getBytes());
      System.out.println(dimensionKey);
    }
  }
}
