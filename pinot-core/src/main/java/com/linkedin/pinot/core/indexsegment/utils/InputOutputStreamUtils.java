package com.linkedin.pinot.core.indexsegment.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.linkedin.pinot.core.data.readers.FileSystemMode;


/**
 *
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 19, 2014
 */
@Deprecated
/**
 * Should not be used, this is carried forward just as an example
 * resources are not closed..
 */
public class InputOutputStreamUtils {
  public static InputStream getDefaultInputStream(String filePath) throws FileNotFoundException {
    return new BufferedInputStream(new FileInputStream(new File(filePath)));
  }

  public static DataOutputStream getDefaultOutputStream(String filePath) throws FileNotFoundException {
    return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(filePath))));
  }

  public static InputStream getInputStream(String filePath, FileSystemMode mode, FileSystem fs) throws IOException {
    InputStream is = null;
    switch (mode) {
      case DISK:
        is = new BufferedInputStream(new FileInputStream(new File(filePath)));
        break;
      case HDFS:

        is = new BufferedInputStream(fs.open(new Path(filePath)));
        break;
      default:
        throw new UnsupportedOperationException();
    }
    return is;
  }

  public static DataOutputStream getOutputStream(String filePath, FileSystemMode mode, FileSystem fs) {
    try {
      DataOutputStream is = null;
      switch (mode) {
        case DISK:
          File newFile = new File(filePath);
          if (!newFile.exists()) {
            if (!newFile.getParentFile().exists()) {
              newFile.getParentFile().mkdirs();
            }
            newFile.createNewFile();
          }
          is = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(filePath))));
          break;
        case HDFS:
          is = fs.create(new Path(filePath));
          break;
        default:
          throw new UnsupportedOperationException();
      }
      return is;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static DataOutputStream getAppendOutputStream(String filePath, FileSystemMode mode, FileSystem fs) {
    try {
      DataOutputStream is = null;
      switch (mode) {
        case DISK:
          File appendFile = new File(filePath);
          if (!appendFile.exists()) {
            if (!appendFile.getParentFile().exists()) {
              appendFile.getParentFile().mkdirs();
            }
            appendFile.createNewFile();
          }
          is = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(appendFile, true)));
          break;
        case HDFS:
          is = fs.append(new Path(filePath));
          break;
        default:
          throw new UnsupportedOperationException();
      }
      return is;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
