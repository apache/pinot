package com.linkedin.thirdeye.api;

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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class to represent an array of dimension values.
 *
 * @author kgopalak
 *
 */
public class DimensionKey implements Comparable<DimensionKey> {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionKey.class);

  static MessageDigest md5;

  static {
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Error initializing md5 message digest toMD5 will fail", e);
    }
  }
  final int hashCode;
  private String[] dimensionValues;

  /**
   *
   * @param dimensionValues
   */
  public DimensionKey(String[] dimensionValues) {
    this.dimensionValues = dimensionValues;
    hashCode = Arrays.hashCode(dimensionValues);
  }

  /**
   *
   * @return
   */
  public String[] getDimensionValues() {
    return dimensionValues;
  }

  public String getDimensionValue(List<DimensionSpec> dimensionSpecs, String dimensionName)
  {
    for (int i = 0; i < dimensionSpecs.size(); i++)
    {
      if (dimensionSpecs.get(i).getName().equals(dimensionName))
      {
        return dimensionValues[i];
      }
    }
    throw new IllegalArgumentException("No dimension " + dimensionName);
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
      byte[] bytes = dimensionValue.getBytes(Charset.forName("utf-8"));
      out.writeInt(bytes.length);
      out.write(bytes);
    }
    baos.close();
    byte[] byteArray = baos.toByteArray();
    try {
      DimensionKey key = fromBytes(byteArray);
    } catch (Exception e) {
      LOG.info("input key:{}", Arrays.toString(dimensionValues));
      LOG.info("generated:{}", Arrays.toString(byteArray));
      throw new RuntimeException(e);
    }
    return byteArray;
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
    try {
      for (int i = 0; i < size; i++) {
        int length = in.readInt();
        byte[] b = new byte[length];
        in.readFully(b);
        dimensionValues[i] = new String(b, "UTF-8");
      }
    } catch (Exception e) {
      LOG.info(Arrays.toString(bytes), e);
      throw new RuntimeException(e);
    }
    return new DimensionKey(dimensionValues);
  }

  public byte[] toMD5() {
    synchronized (md5) {
      byte[] digest = md5.digest(toString().getBytes(Charset.forName("UTF-8")));
      return digest;
    }
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
    return hashCode;
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

  public static void main(String[] args) throws IOException {
    String[] dimensionValues = new String[] { "", "chrome", "gmail.com",
        "android" };
    String[] copy = Arrays.copyOf(dimensionValues, dimensionValues.length);
    copy[0] = "us";
    DimensionKey key = new DimensionKey(dimensionValues);
    System.out.println("tostring--" + key.toString());
    System.out.println(Arrays.toString(key.toBytes()));
    System.out.println(DimensionKey.fromBytes(key.toBytes()));
    String bytesStr = "0, -68, 25, 53, -105, 121, -91, 62, -109, 125, -61, -12, 36, 54, 20, 44, 0, 0, 0, 0, 0, 0, 0, 0";
    String[] split = bytesStr.split(",");
    byte[] bytes = new byte[split.length];
    int idx = 0;
    for (String s : split) {
      bytes[idx] = (byte) Integer.parseInt(s.trim());
      idx = idx + 1;
    }
    DimensionKey dimensionKey = DimensionKey.fromBytes(bytes);
    System.out.println(dimensionKey);
  }

}
