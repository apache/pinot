package com.linkedin.thirdeye.bootstrap.rollup.phase3;

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

public class RollupPhaseThreeOutput {

  MetricTimeSeries timeSeries;

  Set<DimensionKey> dimensionKeySet;

  public RollupPhaseThreeOutput(MetricTimeSeries timeSeries) {
    this.timeSeries = timeSeries;
    dimensionKeySet = new HashSet<DimensionKey>();
  }

  public void addDimensionKey(DimensionKey key) {
    dimensionKeySet.add(key);
  }

  public void addDimensionKeySet(Set<DimensionKey> keySet) {
    this.dimensionKeySet.addAll(keySet);
  }

  public MetricTimeSeries getTimeSeries() {
    return timeSeries;
  }

  public Set<DimensionKey> getDimensionKeySet() {
    return dimensionKeySet;
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
    bytes = timeSeries.toBytes();

    dos.writeInt(bytes.length);
    dos.write(bytes);
    dos.writeInt(dimensionKeySet.size());
    for (DimensionKey key : dimensionKeySet) {
      bytes = key.toBytes();
      dos.writeInt(bytes.length);
      dos.write(bytes);
    }
    baos.close();
    dos.close();
    return baos.toByteArray();
  }

  public static RollupPhaseThreeOutput fromBytes(byte[] bytes,
      MetricSchema schema) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));

    int timeSeriesByteLength = dis.readInt();
    byte[] timeSeriesBytes = new byte[timeSeriesByteLength];
    dis.read(timeSeriesBytes);
    MetricTimeSeries timeSeries;
    timeSeries = MetricTimeSeries.fromBytes(timeSeriesBytes, schema);

    RollupPhaseThreeOutput wrapper;
    wrapper = new RollupPhaseThreeOutput(timeSeries);
    int dimensionKeySetSize = dis.readInt();
    for (int i = 0; i < dimensionKeySetSize; i++) {
      int length = dis.readInt();
      byte[] dimensionKeyBytes = new byte[length];
      dis.read(dimensionKeyBytes);
      DimensionKey dimensionKey = DimensionKey.fromBytes(dimensionKeyBytes);
      wrapper.addDimensionKey(dimensionKey);
    }
    return wrapper;
  }

}
