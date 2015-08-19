package com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

/**
 *
 * @author kgopalak
 *
 */
public class BootstrapPhaseMapOutputKey {

  /**
   * uuid of the leaf node in the star tree
   */
  UUID nodeId;
  /**
   * md5 of the dimension Key
   */
  byte[] md5;

  int hashCode ;

  public BootstrapPhaseMapOutputKey(UUID nodeId, byte[] md5) throws IOException {
    super();
    this.nodeId = nodeId;
    this.md5 = md5;
    this.hashCode = Arrays.hashCode(toBytes());
  }


  public UUID getNodeId() {
    return nodeId;
  }


  public byte[] getMd5() {
    return md5;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    BootstrapPhaseMapOutputKey that = (BootstrapPhaseMapOutputKey) obj;
    return Arrays.equals(this.md5, that.md5) && this.nodeId.equals(that.nodeId);
  }

  public byte[] toBytes() throws IOException{
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    byte[] bytes;
    //write uuid
    bytes = nodeId.toString().getBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);

    //write dimensionKey md5
    dos.writeInt(md5.length);
    dos.write(md5);

    return baos.toByteArray();

  }

  public static BootstrapPhaseMapOutputKey fromBytes(byte[] bytes) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    int length;
    byte[] b;
    //read nodeId
    length = in.readInt();
    b = new byte[length];
    in.readFully(b);
    UUID nodeId = UUID.fromString(new String(b));

    //read md5
    length = in.readInt();
    byte[] md5 = new byte[length];
    in.readFully(md5);
    return new BootstrapPhaseMapOutputKey(nodeId, md5);
  }

}
