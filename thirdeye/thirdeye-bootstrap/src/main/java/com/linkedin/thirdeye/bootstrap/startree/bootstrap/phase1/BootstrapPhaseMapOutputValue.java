package com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;

public class BootstrapPhaseMapOutputValue {



  /**
   * DimensionKey representing the leaf record
   */
  DimensionKey dimensionKey;

  /**
   * Metrics for the key
   */
  MetricTimeSeries metricTimeSeries;

  public BootstrapPhaseMapOutputValue(DimensionKey dimensionKey,
      MetricTimeSeries metricTimeSeries) {
    super();
    this.dimensionKey = dimensionKey;
    this.metricTimeSeries = metricTimeSeries;
  }

  public DimensionKey getDimensionKey() {
    return dimensionKey;
  }

  public MetricTimeSeries getMetricTimeSeries() {
    return metricTimeSeries;
  }

  public byte[] toBytes() throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    byte[] bytes;
    // dimension Key
    bytes = dimensionKey.toBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);
    // raw dimension Key
    bytes = metricTimeSeries.toBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);
    // time series
    baos.close();
    dos.close();
    return baos.toByteArray();
  }

  public static BootstrapPhaseMapOutputValue fromBytes(byte[] buffer,
      MetricSchema schema) throws IOException{
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer));
    int length;
    byte[] bytes;
    // read dimension Key
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    DimensionKey dimensionKey = DimensionKey.fromBytes(bytes);

    // read timeseries
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    MetricTimeSeries timeSeries;
    timeSeries = MetricTimeSeries.fromBytes(bytes, schema);

    return new BootstrapPhaseMapOutputValue(dimensionKey, timeSeries);

  }

}
