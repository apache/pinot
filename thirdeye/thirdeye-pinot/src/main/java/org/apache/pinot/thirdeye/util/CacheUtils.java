package org.apache.pinot.thirdeye.util;

import com.couchbase.client.java.document.json.JsonObject;
import java.util.zip.CRC32;
import org.apache.pinot.thirdeye.detection.cache.TimeSeriesDataPoint;


public class CacheUtils {

  public static String hashMetricUrn(String metricUrn) {
    CRC32 c = new CRC32();
    c.update(metricUrn.getBytes());
    return String.valueOf(c.getValue());
  }

  public static JsonObject buildDocumentStructure(TimeSeriesDataPoint point) {
    JsonObject body = JsonObject.create()
        .put("time", point.getTimestamp())
        .put("metricId", point.getMetricId())
        .put(point.getMetricUrnHash(),
            (point.getDataValue() == null || point.getDataValue().equals("null")) ? "0" : point.getDataValue());
    return body;
  }

  public static String buildQuery(JsonObject parameters) {
    StringBuilder sb = new StringBuilder("SELECT time, `")
        .append(parameters.getString("dimensionKey"))
        .append("` FROM `")
        .append(parameters.getString("bucket"))
        .append("` WHERE metricId = ")
        .append(parameters.getLong("metricId"))
        .append(" AND `")
        .append(parameters.getString("dimensionKey"))
        .append("` IS NOT MISSING")
        .append(" AND time BETWEEN ")
        .append(parameters.getLong("start"))
        .append(" AND ")
        .append(parameters.getLong("end"))
        .append(" ORDER BY time ASC");

    return sb.toString();
  }
}
