package com.linkedin.thirdeye.bootstrap.util;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DFSUtil {

  public static void copy(FileSystem dfs, Path src, Path dst) throws Exception {
    FSDataOutputStream outputStream = null;
    FSDataInputStream inputStream = null;
    try {
      outputStream = dfs.create(dst);
      inputStream = dfs.open(src);
      byte[] buffer = new byte[4096];
      int read;
      while ((read = inputStream.read(buffer)) != -1) {
        outputStream.write(buffer, 0, read);
      }
    } catch (Exception e) {
      throw e;
    } finally {
      if (outputStream != null) {
        outputStream.close();
      }
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }
}
