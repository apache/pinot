package com.linkedin.thirdeye.bootstrap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class to represent an array of dimension values.
 * 
 * @author kgopalak
 * 
 */
public class DimensionKey {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionKey.class);

  static MessageDigest md5;

  static {
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Error initializing md5 message digest toMD5 will fail", e);
    }
  }

  private String[] dimensionValues;

  /**
   * 
   * @param dimensionValues
   */
  public DimensionKey(String[] dimensionValues) {
    this.dimensionValues = dimensionValues;
  }

  /**
   * 
   * @return
   */
  public String[] getDimensionsValues() {
    return dimensionValues;
  }

  /**
   * 
   * @return
   * @throws IOException
   */
  public byte[] toBytes() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    // write the number of dimensions
    out.writeInt(dimensionValues.length);
    // for each dimension write the length of each dimension followed by the
    // values
    for (String dimensionValue : dimensionValues) {
      byte[] bytes = dimensionValue.getBytes("UTF-8");
      out.writeInt(bytes.length);
      out.write(bytes);
    }
    return baos.toByteArray();
  }

  /**
   * 
   * @param bytes
   * @return
   * @throws IOException
   */
  public static DimensionKey fromBytes(byte[] bytes) throws IOException {
    DataInput in = new DataInputStream(new ByteArrayInputStream(bytes));
    // read the number of dimensions
    int size = in.readInt();
    String[] dimensionValues = new String[size];
    // for each dimension read the length of each dimension followed by the
    // values
    for (int i = 0; i < size; i++) {
      int length = in.readInt();
      byte[] b = new byte[length];
      in.readFully(b);
      dimensionValues[i] = new String(b, "UTF-8");
    }
    return new DimensionKey(dimensionValues);
  }

  public byte[] toMD5() {
    return md5.digest(toString().getBytes(Charset.forName("UTF-8")));
  }

  /**
   * 
   * @param that
   * @return
   */
  public int compareTo(DimensionKey that) {
    // assumes both have the same length
    int length = Math.min(this.dimensionValues.length,
        that.dimensionValues.length);
    int ret = 0;
    for (int i = 0; i < length; i++) {
      ret = this.dimensionValues[i].compareTo(that.dimensionValues[i]);
      if (ret != 0) {
        break;
      }
    }
    return ret;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(dimensionValues);
  }

  /**
   * compares to dimensionKey instances
   */
  @Override
  public boolean equals(Object obj) {
    // assumes both have the same length
    if (obj instanceof DimensionKey) {
      DimensionKey that = (DimensionKey) obj;
      int length = Math.min(this.dimensionValues.length,
          that.dimensionValues.length);
      boolean ret = true;
      for (int i = 0; i < length; i++) {
        ret = this.dimensionValues[i].equals(that.dimensionValues[i]);
        if (!ret) {
          break;
        }
      }
      return ret;
    }
    return false;
  }

  @Override
  public String toString() {
    return Arrays.toString(dimensionValues);
  }

  public static void main(String[] args) {
    String[] dimensionValues = new String[] { "", "chrome", "gmail.com",
        "android" };
    DimensionKey key = new DimensionKey(dimensionValues);
    System.out.println("tostring--" + key.toString());
  }

}
