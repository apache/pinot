package com.linkedin.thirdeye.bootstrap.rollup.phase2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;

public class RollupTemp {

  MetricTimeSeries timeSeries;

  Map<DimensionKey, MetricTimeSeries> dimensionMetricTimeSeriesMap;

  public RollupTemp(MetricTimeSeries timeSeries) {
    this.timeSeries = timeSeries;
    this.dimensionMetricTimeSeriesMap = new HashMap<DimensionKey, MetricTimeSeries>();
  }

  public MetricTimeSeries getTimeSeries() {
    return timeSeries;
  }

  public void add(DimensionKey key, MetricTimeSeries series) {
    dimensionMetricTimeSeriesMap.put(key, series);
  }

  /**
   * FORMAT <br>
   * <timeseries length><timeseries byte> <br>
   * <size of dimensionMetricTimeSeriesMap> <br/>
   * for each dimension key set <br>
   * <length of dimension key set bytes> < dimension key bytes>
   */
  public byte[] toBytes() throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    byte[] bytes;
    // aggregate time series
    bytes = timeSeries.toBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);

    // map size
    dos.writeInt(dimensionMetricTimeSeriesMap.size());
    for (Entry<DimensionKey, MetricTimeSeries> entry : dimensionMetricTimeSeriesMap
        .entrySet()) {
      bytes = entry.getKey().toBytes();
      dos.writeInt(bytes.length);
      dos.write(bytes);

      bytes = entry.getValue().toBytes();
      dos.writeInt(bytes.length);
      dos.write(bytes);

    }
    baos.close();
    dos.close();
    return baos.toByteArray();
  }

  public static RollupTemp fromBytes(byte[] bytes,
      MetricSchema schema) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
    //read aggregated time series
    int timeSeriesByteLength = dis.readInt();
    byte[] timeSeriesBytes = new byte[timeSeriesByteLength];
    dis.read(timeSeriesBytes);
    MetricTimeSeries timeSeries;
    timeSeries = MetricTimeSeries.fromBytes(timeSeriesBytes, schema);
    int mapSize = dis.readInt();
    RollupTemp wrapper;
    wrapper = new RollupTemp(timeSeries);
    for (int i = 0; i < mapSize; i++) {
      //read key
      int keyLength = dis.readInt();
      byte[] dimensionKeyBytes = new byte[keyLength];
      dis.read(dimensionKeyBytes);
      DimensionKey dimensionKey = DimensionKey.fromBytes(dimensionKeyBytes);
      //read timeseries
      int seriesLength = dis.readInt();
      byte[] seriesBytes = new byte[seriesLength];
      dis.read(dimensionKeyBytes);
      MetricTimeSeries series = MetricTimeSeries.fromBytes(seriesBytes, schema);
      wrapper.add(dimensionKey, series);

    }


    return wrapper;
  }

}
