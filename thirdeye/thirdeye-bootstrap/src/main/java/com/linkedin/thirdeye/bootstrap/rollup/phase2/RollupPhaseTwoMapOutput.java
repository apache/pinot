package com.linkedin.thirdeye.bootstrap.rollup.phase2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;

public class RollupPhaseTwoMapOutput {

  MetricTimeSeries rawTimeSeries;

  DimensionKey rawDimensionKey;

  DimensionKey rollupDimensionKey;

  public RollupPhaseTwoMapOutput(DimensionKey rollupDimensionKey,
      DimensionKey rawDimensionKey, MetricTimeSeries rawTimeSeries) {
    this.rollupDimensionKey = rollupDimensionKey;
    this.rawTimeSeries = rawTimeSeries;
    this.rawDimensionKey = rawDimensionKey;
  }

  public MetricTimeSeries getRawTimeSeries() {
    return rawTimeSeries;
  }

  public DimensionKey getRawDimensionKey() {
    return rollupDimensionKey;
  }
  public DimensionKey getRollupDimensionKey() {
    return rollupDimensionKey;
  }
  /**
   * FORMAT <br>
   * <timeseries length><timeseries byte> <br>
   * <length of dimension key set> <br/>
   * for each dimension key set <br>
   * <length of dimension key set bytes> < dimension key bytes>
   */
  public byte[] toBytes() throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    byte[] bytes;
    // rollup dimension Key
    bytes = rollupDimensionKey.toBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);
    // raw dimension Key
    bytes = rawDimensionKey.toBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);
    // time series
    bytes = rawTimeSeries.toBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);

    baos.close();
    dos.close();
    return baos.toByteArray();
  }

  public static RollupPhaseTwoMapOutput fromBytes(byte[] buffer,
      MetricSchema schema) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer));
    int length;
    byte[] bytes;
    // rollup dimension Key
     length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    DimensionKey rollupDimensionKey = DimensionKey.fromBytes(bytes);
    // raw dimension Key
     length = dis.readInt();
     bytes = new byte[length];
    dis.read(bytes);
    DimensionKey rawDimensionKey = DimensionKey.fromBytes(bytes);
    // read raw timeseries
    length= dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    MetricTimeSeries timeSeries;
    timeSeries = MetricTimeSeries.fromBytes(bytes, schema);

    RollupPhaseTwoMapOutput wrapper;
    wrapper = new RollupPhaseTwoMapOutput(rollupDimensionKey, rawDimensionKey, timeSeries);
    return wrapper;
  }

}
