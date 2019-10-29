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

  private Bucket bucket;

  public CouchbaseCacheDAO() {
    this.createDataStoreConnection();
  }

  private void createDataStoreConnection() {
    if (CacheConfig.useCentralizedCache()) {
      Cluster cluster = CouchbaseCluster.create();
      cluster.authenticate(CacheConfig.COUCHBASE_AUTH_USERNAME, CacheConfig.COUCHBASE_AUTH_PASSWORD);
      this.bucket = cluster.openBucket(CacheConfig.COUCHBASE_BUCKET_NAME);
    }
  }

  public ThirdEyeCacheResponse tryFetchExistingTimeSeries(ThirdEyeCacheRequest request) {

    String dimensionKey = request.getDimensionKey();

    // NOTE: we subtract 1 granularity from the end date because Couchbase's BETWEEN clause is inclusive on both sides
    JsonObject parameters = JsonObject.create()
        .put(CacheConstants.BUCKET, CacheConfig.COUCHBASE_BUCKET_NAME)
        .put(CacheConstants.METRIC_ID, request.getMetricId())
        .put(CacheConstants.DIMENSION_KEY, request.getDimensionKey())
        .put(CacheConstants.START, request.getStartTimeInclusive())
        .put(CacheConstants.END, request.getEndTimeExclusive() - request.getRequest().getGroupByTimeGranularity().toMillis());

    String query = CacheUtils.buildQuery(parameters);

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(query));

    if (!queryResult.finalSuccess()) {
      LOG.error("cache error occurred for window startTime = {} to endTime = {}", request.getStartTimeInclusive(), request.getEndTimeExclusive());
      return null;
    }

    List<TimeSeriesDataPoint> timeSeriesRows = new ArrayList<>();

    // move this out of DAO and put it in the TimeSeriesCache instead?
    // otherwise hard to test.

    for (N1qlQueryRow row : queryResult) {
      long timestamp = row.value().getLong(CacheConstants.TIME);
      String dataValue = row.value().getString(dimensionKey);
      timeSeriesRows.add(new TimeSeriesDataPoint(request.getMetricUrn(), timestamp, request.getMetricId(), dataValue));
    }

    return new ThirdEyeCacheResponse(request, timeSeriesRows);
  }

  public void insertTimeSeriesDataPoint(TimeSeriesDataPoint point) {

    // will this have memory issues later? if we need to pull all the dimension values into memory.
    JsonDocument doc = bucket.getAndTouch(point.getDocumentKey(), CacheConfig.TTL);

    if (doc == null) {
      JsonObject documentBody = CacheUtils.buildDocumentStructure(point);
      doc = JsonDocument.create(point.getDocumentKey(), CacheConfig.TTL, documentBody);
    } else {
      JsonObject dimensions = doc.content();
      if (dimensions.containsKey(point.getMetricUrnHash()))
        return;
      dimensions.put(point.getMetricUrnHash(),
          (point.getDataValue() == null || point.getDataValue().equals("null")) ? "0" : point.getDataValue());
    }

    bucket.upsert(doc);
  }
}
