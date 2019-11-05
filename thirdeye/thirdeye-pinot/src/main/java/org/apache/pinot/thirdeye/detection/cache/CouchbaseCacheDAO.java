/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.util.CacheUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DAO class used to fetch data from Couchbase.
 */

public class CouchbaseCacheDAO {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseCacheDAO.class);

  private Bucket bucket;

  public CouchbaseCacheDAO() {
    if (CacheConfig.useCentralizedCache()) {
      this.createDataStoreConnection();
    }
  }

  /**
   * Initialize connection to Couchbase and open bucket where data is stored.
   */
  private void createDataStoreConnection() {
    Cluster cluster = CouchbaseCluster.create();

    cluster.authenticate(CacheConfig.getAuthUsername(), CacheConfig.getAuthPassword());
    this.bucket = cluster.openBucket(CacheConfig.getBucketName());
  }

  /**
   * Fetches time-series data from cache. Formats it into a list of TimeSeriesDataPoints before
   * returning. The raw results will look something like this:
   * {
   *   {
   *     "time": 1000,
   *     "123456": "30.0"
   *   },
   *   {
   *     "time": 2000,
   *     "123456": "893.0"
   *   },
   *   {
   *     "time": 3000,
   *     "123456": "900.6"
   *   }
   * }
   * @param request ThirdEyeCacheRequest. Wrapper for ThirdEyeRequest.
   * @return list of TimeSeriesDataPoint
   * @throws Exception if query threw an error
   */

  // NOTE: this will be slow unless indexes are created on "time" and "metricId".
  public ThirdEyeCacheResponse tryFetchExistingTimeSeries(ThirdEyeCacheRequest request) throws Exception {
    String dimensionKey = request.getDimensionKey();

    // NOTE: we subtract 1 granularity from the end date because Couchbase's BETWEEN clause is inclusive on both sides
    JsonObject parameters = JsonObject.create()
        .put(CacheConstants.BUCKET, CacheConfig.getBucketName())
        .put(CacheConstants.METRIC_ID, request.getMetricId())
        .put(CacheConstants.DIMENSION_KEY, request.getDimensionKey())
        .put(CacheConstants.START, request.getStartTimeInclusive())
        .put(CacheConstants.END, request.getEndTimeExclusive() - request.getRequest().getGroupByTimeGranularity().toMillis());

    String query = CacheUtils.buildQuery(parameters);

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(query));

    ThirdeyeMetricsUtil.couchbaseCallCounter.inc();

    if (!queryResult.finalSuccess()) {
      LOG.error("cache error occurred for window startTime = {} to endTime = {}", request.getStartTimeInclusive(), request.getEndTimeExclusive());

      for (JsonObject error : queryResult.errors()) {
        LOG.error(error.getString("msg"));
      }

      ThirdeyeMetricsUtil.couchbaseExceptionCounter.inc();
      throw new Exception("query to Couchbase failed");
    }

    List<TimeSeriesDataPoint> timeSeriesRows = new ArrayList<>();

    for (N1qlQueryRow row : queryResult) {
      long timestamp = row.value().getLong(CacheConstants.TIME);
      String dataValue = row.value().getString(dimensionKey);
      timeSeriesRows.add(new TimeSeriesDataPoint(request.getMetricUrn(), timestamp, request.getMetricId(), dataValue));
    }

    return new ThirdEyeCacheResponse(request, timeSeriesRows);
  }

  /**
   * Insert a TimeSeriesDataPoint into Couchbase. If a document for this data point already
   * exists within Couchbase and the data value for the metricURN already exists in the cache,
   * we don't do anything. An example document generated and inserted for this might look like:
   * {
   *   "time": 123456700000
   *   "metricId": 123456,
   *   "61427020": "3.0",
   *   "83958352": "59.6",
   *   "98648743": "0.0"
   * }
   * @param point data point
   */
  public void insertTimeSeriesDataPoint(TimeSeriesDataPoint point) {

    JsonDocument doc = bucket.getAndTouch(point.getDocumentKey(), CacheConfig.getCentralizedCacheSettings().getTTL());
    ThirdeyeMetricsUtil.couchbaseCallCounter.inc();

    if (doc == null) {
      JsonObject documentBody = CacheUtils.buildDocumentStructure(point);
      doc = JsonDocument.create(point.getDocumentKey(), CacheConfig.getCentralizedCacheSettings().getTTL(), documentBody);
    } else {
      JsonObject dimensions = doc.content();
      if (dimensions.containsKey(point.getMetricUrnHash())) {
        return;
      }

      dimensions.put(point.getMetricUrnHash(), point.getDataValue());
    }

    bucket.upsert(doc);
    ThirdeyeMetricsUtil.couchbaseWriteCounter.inc();
  }
}
