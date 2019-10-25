package org.apache.pinot.thirdeye.detection.cache;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.thirdeye.util.CacheUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CouchbaseCacheDAO {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseCacheDAO.class);

  // these will all be moved to config files
  private static final String AUTH_USERNAME = "thirdeye";
  private static final String AUTH_PASSWORD = "thirdeye";
  private static final String BUCKET_NAME = "travel-sample";

  private static final String BUCKET = "bucket";
  private static final String DIMENSION_KEY = "dimensionKey";
  private static final String METRIC_ID = "metricId";
  private static final String START = "start";
  private static final String END = "end";

  private static final String TIME = "time";

  // 1 hour
  private static final int TIMEOUT = 3600;

  private Bucket bucket;

  public CouchbaseCacheDAO() {
    this.createDataStoreConnection();
  }

  private void createDataStoreConnection() {
    Cluster cluster = CouchbaseCluster.create();
    cluster.authenticate(AUTH_USERNAME, AUTH_PASSWORD);
    this.bucket = cluster.openBucket(BUCKET_NAME);
  }

  public ThirdEyeCacheResponse tryFetchExistingTimeSeries(ThirdEyeCacheRequest request) {

    String dimensionKey = request.getDimensionKey();

    // NOTE: we subtract one from the end date because Couchbase's BETWEEN clause is inclusive on both sides
    JsonObject parameters = JsonObject.create()
        .put(BUCKET, BUCKET_NAME)
        .put(METRIC_ID, request.getMetricId())
        .put(DIMENSION_KEY, request.getDimensionKey())
        .put(START, request.getStartTimeInclusive())
        .put(END, request.getEndTimeExclusive() - 1);

    String query = CacheUtils.buildQuery(parameters);

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(query));

    if (!queryResult.finalSuccess()) {
      LOG.error("cache error occurred for window startTime = {} to endTime = {}", request.getStartTimeInclusive(), request.getEndTimeExclusive());
      return null;
    }

    List<TimeSeriesDataPoint> timeSeriesRows = new ArrayList<>();

    for (N1qlQueryRow row : queryResult) {
      long timestamp = row.value().getLong(TIME);
      String dataValue = row.value().getString(dimensionKey);
      timeSeriesRows.add(new TimeSeriesDataPoint(request.getMetricUrn(), timestamp, request.getMetricId(), dataValue));
    }

    return new ThirdEyeCacheResponse(request, timeSeriesRows);
  }

  public void insertTimeSeriesDataPoint(TimeSeriesDataPoint point) {

    JsonDocument doc = bucket.getAndTouch(point.getDocumentKey(), TIMEOUT);

    if (doc == null) {
      JsonObject documentBody = CacheUtils.buildDocumentStructure(point);
      doc = JsonDocument.create(point.getDocumentKey(), TIMEOUT, documentBody);
    } else {
      JsonObject dimensions = doc.content().getObject("dims");
      if (dimensions.containsKey(point.getMetricUrnHash()))
        return;
      dimensions.put(point.getMetricUrnHash(), point.getDataValue() == null ? "0" : point.getDataValue());
    }

    bucket.upsert(doc);
  }
}
