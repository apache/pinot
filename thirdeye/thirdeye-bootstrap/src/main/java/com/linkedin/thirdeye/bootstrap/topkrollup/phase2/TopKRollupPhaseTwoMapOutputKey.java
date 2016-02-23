package com.linkedin.thirdeye.bootstrap.topkrollup.phase2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TopKRollupPhaseTwoMapOutputKey {

  String dimensionName;

  public TopKRollupPhaseTwoMapOutputKey(String dimensionName) {

    this.dimensionName = dimensionName;
  }

  public String getDimensionName() {
    return dimensionName;
  }

  public byte[] toBytes() throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    byte[] bytes;
    // dimension value
    bytes = dimensionName.getBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);

    baos.close();
    dos.close();
    return baos.toByteArray();
  }

  public static TopKRollupPhaseTwoMapOutputKey fromBytes(byte[] buffer) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer));
    int length;
    byte[] bytes;
    // dimension name
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    String dimensionName = new String(bytes);

    TopKRollupPhaseTwoMapOutputKey wrapper;
    wrapper = new TopKRollupPhaseTwoMapOutputKey(dimensionName);
    return wrapper;
  }

}
