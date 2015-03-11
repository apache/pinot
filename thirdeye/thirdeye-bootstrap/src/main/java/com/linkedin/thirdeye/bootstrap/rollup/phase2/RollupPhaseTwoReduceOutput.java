package com.linkedin.thirdeye.bootstrap.rollup.phase2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;

public class RollupPhaseTwoReduceOutput {

  DimensionKey rollupDimensionKey;

  MetricTimeSeries rollupTimeSeries;

  MetricTimeSeries rawTimeSeries;

  DimensionKey rawDimensionKey;

  /**
   *
   * @param rollupDimensionKey
   * @param rolledupTimeSeries
   * @param rawTimeSeries
   */
  public RollupPhaseTwoReduceOutput(DimensionKey rollupDimensionKey,
      MetricTimeSeries rollupTimeSeries, DimensionKey rawDimensionKey,
      MetricTimeSeries rawTimeSeries) {
    super();
    this.rollupDimensionKey = rollupDimensionKey;
    this.rollupTimeSeries = rollupTimeSeries;
    this.rawDimensionKey = rawDimensionKey;
    this.rawTimeSeries = rawTimeSeries;
  }

  public DimensionKey getRollupDimensionKey() {
    return rollupDimensionKey;
  }

  public MetricTimeSeries getRollupTimeSeries() {
    return rollupTimeSeries;
  }

  public DimensionKey getRawDimensionKey() {
    return rawDimensionKey;
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

    // raw dimension Key

    bytes = rawDimensionKey.toBytes();
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

    // roll up dimension Key
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    DimensionKey rollupDimensionKey = DimensionKey.fromBytes(bytes);
    // read rollup timeseries
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    MetricTimeSeries rollupTimeSeries;
    rollupTimeSeries = MetricTimeSeries.fromBytes(bytes, schema);

    // raw dimension Key
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    DimensionKey rawDimensionKey = DimensionKey.fromBytes(bytes);

    // read raw timeseries
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    MetricTimeSeries rawTimeSeries;
    rawTimeSeries = MetricTimeSeries.fromBytes(bytes, schema);

    return new RollupPhaseTwoReduceOutput(rollupDimensionKey, rollupTimeSeries,
        rawDimensionKey, rawTimeSeries);
  }
}
