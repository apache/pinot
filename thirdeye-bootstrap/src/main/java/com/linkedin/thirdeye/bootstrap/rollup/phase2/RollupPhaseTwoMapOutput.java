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

public class RollupPhaseTwoMapOutput {

  MetricTimeSeries timeSeries;

  DimensionKey dimensionKey;

  public RollupPhaseTwoMapOutput(DimensionKey dimensionKey,
      MetricTimeSeries timeSeries) {
    this.timeSeries = timeSeries;
    this.dimensionKey = dimensionKey;
  }

  public MetricTimeSeries getTimeSeries() {
    return timeSeries;
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
    //dimension Key
    bytes = dimensionKey.toBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);
    //time series
    bytes = timeSeries.toBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);
   
    baos.close();
    dos.close();
    return baos.toByteArray();
  }

  public static RollupPhaseTwoMapOutput fromBytes(byte[] bytes,
      MetricSchema schema) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
    //dimension Key
    int length = dis.readInt();
    byte[] dimensionKeyBytes = new byte[length];
    dis.read(dimensionKeyBytes);
    DimensionKey dimensionKey = DimensionKey.fromBytes(dimensionKeyBytes);
    //read timeseries
    int timeSeriesByteLength = dis.readInt();
    byte[] timeSeriesBytes = new byte[timeSeriesByteLength];
    dis.read(timeSeriesBytes);
    MetricTimeSeries timeSeries;
    timeSeries = MetricTimeSeries.fromBytes(timeSeriesBytes, schema);
   
    RollupPhaseTwoMapOutput wrapper;
    wrapper = new RollupPhaseTwoMapOutput(dimensionKey, timeSeries);
    return wrapper;
  }

}
