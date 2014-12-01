package com.linkedin.thirdeye.bootstrap.rollup.phase2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.linkedin.thirdeye.bootstrap.DimensionKey;
import com.linkedin.thirdeye.bootstrap.MetricSchema;
import com.linkedin.thirdeye.bootstrap.MetricTimeSeries;

public class RollupPhaseTwoReduceOutput {

  DimensionKey rollupDimensionKey;

  MetricTimeSeries rollupTimeSeries;

  MetricTimeSeries rawTimeSeries;

  /**
   * 
   * @param rollupDimensionKey
   * @param rolledupTimeSeries
   * @param rawTimeSeries
   */
  public RollupPhaseTwoReduceOutput(DimensionKey rollupDimensionKey,
      MetricTimeSeries rollupTimeSeries, MetricTimeSeries rawTimeSeries) {
    super();
    this.rollupDimensionKey = rollupDimensionKey;
    this.rollupTimeSeries = rollupTimeSeries;
    this.rawTimeSeries = rawTimeSeries;
  }

  public DimensionKey getRollupDimensionKey() {
    return rollupDimensionKey;
  }

  public MetricTimeSeries getRollupTimeSeries() {
    return rollupTimeSeries;
  }

  public MetricTimeSeries getRawTimeSeries() {
    return rawTimeSeries;
  }

  public byte[] toBytes() throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    byte[] bytes;

    // rolled up dimension Key
    bytes = rollupDimensionKey.toBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);
    // rolled up time series
    bytes = rollupTimeSeries.toBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);

    // raw time series
    bytes = rawTimeSeries.toBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);

    baos.close();
    dos.close();
    return baos.toByteArray();
  }

  public static RollupPhaseTwoReduceOutput fromBytes(byte[] buffer,
      MetricSchema schema) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer));
    int length;
    byte[] bytes;

    // dimension Key
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    DimensionKey rollupDimensionKey = DimensionKey.fromBytes(bytes);
    // read timeseries
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    MetricTimeSeries rollupTimeSeries;
    rollupTimeSeries = MetricTimeSeries.fromBytes(bytes, schema);

    // read timeseries
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    MetricTimeSeries rawTimeSeries;
    rawTimeSeries = MetricTimeSeries.fromBytes(bytes, schema);

    return new RollupPhaseTwoReduceOutput(rollupDimensionKey, rollupTimeSeries,
        rawTimeSeries);
  }
}
