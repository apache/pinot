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
import com.couchbase.client.java.auth.CertAuthenticator;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.util.CacheUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DAO class used to fetch data from Couchbase.
 */

public class CouchbaseCacheDAO implements CacheDAO {

  private static final String USE_CERT_BASED_AUTH = "useCertificateBasedAuthentication";
  private static final String HOSTS = "hosts";
  private static final String BUCKET_NAME = "bucketName";
  private static final String AUTH_USERNAME = "authUsername";
  private static final String AUTH_PASSWORD = "authPassword";
  private static final String ENABLE_DNS_SRV = "enableDnsSrv";
  private static final String KEY_STORE_FILE_PATH = "keyStoreFilePath";
  private static final String KEY_STORE_PASSWORD = "keyStorePassword";
  private static final String TRUST_STORE_FILE_PATH = "trustStoreFilePath";
  private static final String TRUST_STORE_PASSWORD = "trustStorePassword";

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseCacheDAO.class);

  private Bucket bucket;

  public CouchbaseCacheDAO() {
    this.createDataStoreConnection();
  }

  /**
   * Initialize connection to Couchbase and open bucket where data is stored.
   */
  private void createDataStoreConnection() {
    CacheDataSource dataSource = CacheConfig.getInstance().getCentralizedCacheSettings().getDataSourceConfig();
    Map<String, Object> config = dataSource.getConfig();
    List<String> hosts = ConfigUtils.getList(config.get(HOSTS));

    Cluster cluster;
    if (MapUtils.getBoolean(config, USE_CERT_BASED_AUTH)) {
      CouchbaseEnvironment env = DefaultCouchbaseEnvironment
          .builder()
          .sslEnabled(true)
          .certAuthEnabled(true)
          .dnsSrvEnabled(MapUtils.getBoolean(config, ENABLE_DNS_SRV))
          .sslKeystoreFile(MapUtils.getString(config, KEY_STORE_FILE_PATH))
          .sslKeystorePassword(MapUtils.getString(config, KEY_STORE_PASSWORD))
          .sslTruststoreFile(MapUtils.getString(config, TRUST_STORE_FILE_PATH))
          .sslTruststorePassword(MapUtils.getString(config, TRUST_STORE_PASSWORD))
          .build();

      cluster = CouchbaseCluster.create(env, CacheUtils.getBootstrapHosts(hosts));
      cluster.authenticate(CertAuthenticator.INSTANCE);
    } else {
      cluster = CouchbaseCluster.create(hosts);
      cluster.authenticate(MapUtils.getString(config, AUTH_USERNAME), MapUtils.getString(config, AUTH_PASSWORD));
    }

    this.bucket = cluster.openBucket(CacheUtils.getBucketName());
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
    String bucketName = CacheUtils.getBucketName();
    String dimensionKey = request.getDimensionKey();

    // NOTE: we subtract 1 granularity from the end date because Couchbase's BETWEEN clause is inclusive on both sides
    JsonObject parameters = JsonObject.create()
        .put(CacheConstants.BUCKET, bucketName)
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
      long timestamp = row.value().getLong(CacheConstants.TIMESTAMP);
      Double dataValue = row.value().getDouble(dimensionKey);
      timeSeriesRows.add(new TimeSeriesDataPoint(request.getMetricUrn(), timestamp, request.getMetricId(), String.valueOf(dataValue)));
    }

    return new ThirdEyeCacheResponse(request, timeSeriesRows);
  }

  /**
   * Insert a TimeSeriesDataPoint into Couchbase. If a document for this data point already
   * exists within Couchbase and the data value for the metricURN already exists in the cache,
   * we don't do anything. An example document generated and inserted for this might look like:
   * {
   *   "timestamp": 123456700000
   *   "metricId": 123456,
   *   "61427020": "3.0",
   *   "83958352": "59.6",
   *   "98648743": "0.0"
   * }
   * @param point data point
   */
  public void insertTimeSeriesDataPoint(TimeSeriesDataPoint point) {

    JsonDocument doc = bucket.getAndTouch(point.getDocumentKey(), CacheConfig.getInstance().getCentralizedCacheSettings().getTTL());
    ThirdeyeMetricsUtil.couchbaseCallCounter.inc();

    if (doc == null) {
      JsonObject documentBody = CacheUtils.buildDocumentStructure(point);
      doc = JsonDocument.create(point.getDocumentKey(), CacheConfig.getInstance().getCentralizedCacheSettings().getTTL(), documentBody);
    } else {
      JsonObject dimensions = doc.content();
      if (dimensions.containsKey(point.getMetricUrnHash())) {
        return;
      }

      dimensions.put(point.getMetricUrnHash(), point.getDataValueAsDouble());
    }

    bucket.upsert(doc);
    ThirdeyeMetricsUtil.couchbaseWriteCounter.inc();
  }
}
