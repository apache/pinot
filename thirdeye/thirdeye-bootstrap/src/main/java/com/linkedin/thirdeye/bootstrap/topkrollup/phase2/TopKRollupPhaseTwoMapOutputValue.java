package com.linkedin.thirdeye.bootstrap.topkrollup.phase2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;

public class TopKRollupPhaseTwoMapOutputValue {

  String dimensionValue;
  MetricTimeSeries series;

  public TopKRollupPhaseTwoMapOutputValue(String dimensionValue, MetricTimeSeries series) {

    this.dimensionValue = dimensionValue;
    this.series = series;
  }

  public MetricTimeSeries getSeries() {
    return series;
  }

  public String getDimensionValue() {
    return dimensionValue;
  }

  public byte[] toBytes() throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    byte[] bytes;
    // dimension value
    bytes = dimensionValue.getBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);
    // series
    bytes = series.toBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);

    baos.close();
    dos.close();
    return baos.toByteArray();
  }

  public static TopKRollupPhaseTwoMapOutputValue fromBytes(byte[] buffer, MetricSchema schema) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer));
    int length;
    byte[] bytes;
    // dimension value
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    String dimensionValue = new String(bytes);
    // series
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    MetricTimeSeries series = MetricTimeSeries.fromBytes(bytes, schema);

    TopKRollupPhaseTwoMapOutputValue wrapper;
    wrapper = new TopKRollupPhaseTwoMapOutputValue(dimensionValue, series);
    return wrapper;
  }

}
