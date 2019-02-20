/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.common.dimension;

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
 * @author kgopalak
 */
public class DimensionKey implements Comparable<DimensionKey> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionKey.class);

  static MessageDigest md5;

  static {
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOGGER.error("Error initializing md5 message digest toMD5 will fail", e);
    }
  }

  final int hashCode;
  private String[] dimensionValues;

  /**
   * @param dimensionValues
   */
  public DimensionKey(String[] dimensionValues) {
    this.dimensionValues = dimensionValues;
    hashCode = Arrays.hashCode(dimensionValues);
  }

  /**
   * @return
   */
  public String[] getDimensionValues() {
    return dimensionValues;
  }

  /**
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
      LOGGER.info("input key:{}", Arrays.toString(dimensionValues));
      LOGGER.info("generated:{}", Arrays.toString(byteArray));
      throw new RuntimeException(e);
    }
    return byteArray;
  }

  /**
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
      LOGGER.info(Arrays.toString(bytes), e);
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
   * @param that
   * @return
   */
  public int compareTo(DimensionKey that) {
    // assumes both have the same length
    int length = Math.min(this.dimensionValues.length, that.dimensionValues.length);
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
      int length = Math.min(this.dimensionValues.length, that.dimensionValues.length);
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

  public String toCommaSeparatedString() {
    if (dimensionValues == null) {
      return null;
    }
    if (dimensionValues.length == 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    String separator = "";
    for (String s : dimensionValues) {
      sb.append(separator).append(s);
      separator = ",";
    }
    return sb.toString();
  }
}
