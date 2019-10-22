package org.apache.pinot.integration.tests;

import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.pinot.filesystem.HadoopPinotFS;


public class MockHadoopPinotFS extends HadoopPinotFS {
  public MockHadoopPinotFS() {

  }
  @Override
  public void init(Configuration config) {
    org.apache.hadoop.conf.Configuration hdfsClientCfg = new org.apache.hadoop.conf.Configuration();
    Iterator keyItr = config.getKeys();
    while(keyItr.hasNext()) {
      Object key = keyItr.next();
      hdfsClientCfg.set(key.toString(), config.getString(key.toString()));
    }
    try {
      _hadoopFS = FileSystem.get(hdfsClientCfg);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return;
  }
}
