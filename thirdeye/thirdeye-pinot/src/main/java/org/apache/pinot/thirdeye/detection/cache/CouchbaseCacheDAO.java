package org.apache.pinot.thirdeye.detection.cache;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.subdoc.DocumentFragment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.auto.onboard.AutoOnboardUtility;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.RelationalThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.util.CacheUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CouchbaseCacheDAO {

  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardUtility.class);

  // these will all be moved to config files
  //private static final String HOSTNAME = "localhost";
  private static final String AUTH_USERNAME = "thirdeye";
  private static final String AUTH_PASSWORD = "thirdeye";
  private static final String BUCKET_NAME = "thirdeye_test";

  private static final String METRICS_KEY = "metrics";
  private static final String START_KEY = "start";
  private static final String END_KEY = "end";

  private static final String DATE_KEY = "Date";

  private static final int TIMEOUT = 36000;

  private Bucket bucket;

  public CouchbaseCacheDAO() {
    this.createDataStoreConnection();
  }

  private void createDataStoreConnection() {
    Cluster cluster = CouchbaseCluster.create();
    cluster.authenticate(AUTH_USERNAME, AUTH_PASSWORD);

    this.bucket = cluster.openBucket(BUCKET_NAME);
  }

  public boolean checkIfDetectionIdExistsInCache(String detectionId) {
    return bucket.exists(String.valueOf(detectionId));
  }

  // rework this once we figure out how we're going to store data in couchbase

  //public MetricCacheResponse fetchExistingTimeSeries(long detectionId, MetricSlice slice) throws Exception {
  public ThirdEyeCacheResponse tryFetchExistingTimeSeries(ThirdEyeCacheRequestContainer request) {
    JsonDocument doc = bucket.get(request.getDetectionId());

    if (doc == null) {
      LOG.info("cache fetch for detection {} failed, retrieving from source", request.getDetectionId());
      return null;
    }

    return CacheUtils.mapJsonToCacheResponse(doc.content());
  }

  public void insertRelationalTimeSeries(String detectionId, ThirdEyeResponse responseData) {

    List<List<String>> jsonRows = new ArrayList<>();
    List<String[]> rowList = new ArrayList<>();

    // make a copy of the rows to avoid messing with the original
    for (String[] row : ((RelationalThirdEyeResponse)responseData).getRows()) {
      rowList.add(row);
    }

    Collections.sort(rowList, (a, b) -> Long.valueOf(a[0]).compareTo(Long.valueOf(b[0])));

    // we use Arrays.asList because Couchbase doesn't support primitives.
    for (String[] row : rowList) {
      jsonRows.add(Arrays.asList(row));
    }

    TimeSpec timeSpec = responseData.getDataTimeSpec();

    Map<String, String> timeSpecMap = new HashMap<String, String>() {{
      put("columnName", timeSpec.getColumnName());
      put("dataGranularity", timeSpec.getDataGranularity().toString());
      put("format", timeSpec.getFormat());
    }};

    JsonObject jsonObject = JsonObject.create()
        .put(START_KEY, String.valueOf(responseData.getRequest().getStartTimeInclusive().getMillis()))
        .put(END_KEY, String.valueOf(responseData.getRequest().getEndTimeExclusive().getMillis()))
        .put(METRICS_KEY, jsonRows)
        .put("timeSpec", timeSpecMap);

    // this function needs to take in detection id as well and make that the key.
    bucket.upsert(JsonDocument.create(detectionId, TIMEOUT, jsonObject));

  }
}
