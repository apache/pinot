package org.apache.pinot.thirdeye.detection.cache;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.SimpleN1qlQuery;
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
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.RelationalThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.CacheUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


public class CouchbaseCacheDAO {

  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardUtility.class);

  // these will all be moved to config files
  //private static final String HOSTNAME = "localhost";
  private static final String AUTH_USERNAME = "thirdeye";
  private static final String AUTH_PASSWORD = "thirdeye";
  private static final String BUCKET_NAME = "travel-sample";

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

  public boolean checkIfDetectionIdExistsInCache(String key) {
    return false;
    //return bucket.exists.(key);
  }

  // rework this once we figure out how we're going to store data in couchbase

  //public MetricCacheResponse fetchExistingTimeSeries(long detectionId, MetricSlice slice) throws Exception {
  public ThirdEyeCacheResponse tryFetchExistingTimeSeries(ThirdEyeRequest request) {
//    //JsonDocument doc = bucket.get(request.getDetectionId());
//
//    // parametrize this later
//
//    // need to figure out how detections with multiple metrics will be stored
//
//    StringBuilder sb = new StringBuilder("SELECT `time`, `value` FROM `" + BUCKET_NAME + "` WHERE `metric_name` = `");
//    List<String> metricNames = request.getMetricNames();
//
//    for (int i = 0; i < metricNames.size() - 1; i++) {
//      sb.append(metricNames.get(i)).append("` OR `metric_name` = `");
//    }
//
//    // either this or use the metricId in the request instead
//    sb.append(metricNames.get(metricNames.size() - 1) + "`");
//    sb.append(" AND dataset = " + request.getDataSource());
//    sb.append(" AND time BETWEEN ");
//    sb.append("\"" + request.getStartTimeInclusive().toString() + "\"");
//    sb.append(" AND ");
//    sb.append("\"" + request.getEndTimeExclusive().toString() + "\"");
//    sb.append(" ORDER BY date asc;");
//
//    N1qlQueryResult result = bucket.query(N1qlQuery.simple(sb.toString()));
//
//    // if query failed or no results were returned
//    if (!result.finalSuccess() || result.allRows().isEmpty()) {
//      LOG.info("cache fetch missed or errored, retrieving data from source");
//      return null;
//    }
//
//    List<String[]> rowList = new ArrayList<>();
//
//    int i = 0;
//    DateTime startDate = request.getStartTimeInclusive();
//    Period period = request.getGroupByTimeGranularity().toPeriod();
//
//    // TODO: figure out a way to get around the relational ThirdEyeResponse data not having dates
//
//    //startDate.withPeriodAdded()
//    for (N1qlQueryRow row : result.allRows()) {
//      JsonObject dataPoint = row.value();
//      String[] pair = new String[2];
//
//      // since it's in sorted order, time bucket id will just be equal to the counter.
//      // however, we can consider changing the query to not return in sorted order
//      // and just store the time bucket value with it, which should also work.
//      String bucketNumber = String.valueOf(i);
//      String value = String.valueOf(dataPoint.get("value"));
//
//      // ignore "bubbles"/"nops"/nulls, since they represent missing data in the source
//      if (!value.equals("null")) {
//        pair[0] = bucketNumber;
//        pair[1] = value;
//        rowList.add(pair);
//      }
//    }

    return null;
  }

  public void insertRelationalTimeSeries(ThirdEyeResponse responseData) {

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
    bucket.upsert(JsonDocument.create("0", TIMEOUT, jsonObject));

  }

  public void insertTimeSeriesDataPoint(TimeSeriesDataPoint point) {

//    JsonDocument doc = bucket.getAndTouch(point.getDocumentKey(), TIMEOUT);
    bucket.async().getAndTouch(point.getDocumentKey(), TIMEOUT)
        .subscribe(doc -> {
          if (doc == null) {
            // if data point doesn't exist in cache, make a new document and insert it
            JsonObject documentBody = CacheUtils.buildDocumentStructure(point);
            doc = JsonDocument.create(point.getDocumentKey(), TIMEOUT, documentBody);
          } else {
            ((JsonObject)doc.content().get("dims")).put(point.getDimensionKey(), point.getDataValue());
          }

          bucket.async().upsert(doc);
        }
    );

//    if (doc == null) {
//      // if data point doesn't exist in cache, make a new document and insert it
//      JsonObject documentBody = CacheUtils.buildDocumentStructure(point);
//      doc = JsonDocument.create(point.getDocumentKey(), TIMEOUT, documentBody);
//    } else {
//      ((JsonObject)doc.content().get("dims")).put(point.getDimensionKey(), point.getDataValue());
//    }
//
//    bucket.async().upsert(doc);
  }
}
