package com.linkedin.thirdeye.client.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.pinot.PinotQuery;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.dashboard.Utils;

public class CollectionMaxDataTimeCacheLoader extends CacheLoader<String, Long> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionMaxDataTimeCacheLoader.class);
  private final static String COLLECTION_MAX_TIME_QUERY_TEMPLATE = "SELECT max(%s) FROM %s WHERE %s >= %s";

  private final LoadingCache<String, CollectionSchema> collectionSchemaCache;
  private final LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache;

  private final Map<String, Long> collectionToPrevMaxDataTimeMap = new ConcurrentHashMap<String, Long>();

  public CollectionMaxDataTimeCacheLoader(PinotThirdEyeClientConfig pinotThirdEyeClientConfig,
      LoadingCache<String, CollectionSchema> collectionSchemaCache, LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache) {
    this.collectionSchemaCache = collectionSchemaCache;
    this.resultSetGroupCache = resultSetGroupCache;
  }

  @Override
  public Long load(String collection) throws Exception {
    LOGGER.info("Loading maxDataTime cache {}", collection);
    long maxTime = 0;
    try {
      TimeSpec timeSpec = collectionSchemaCache.get(collection).getTime();
      long prevMaxDataTime = getPrevMaxDataTime(collection, timeSpec);
      String maxTimePql = String.format(COLLECTION_MAX_TIME_QUERY_TEMPLATE, timeSpec.getColumnName(), collection, timeSpec.getColumnName(),
          prevMaxDataTime);
      PinotQuery maxTimePinotQuery = new PinotQuery(maxTimePql, collection);
      resultSetGroupCache.refresh(maxTimePinotQuery);
      ResultSetGroup resultSetGroup = resultSetGroupCache.get(maxTimePinotQuery);
      if (resultSetGroup.getResultSetCount() == 0 || resultSetGroup.getResultSet(0).getRowCount() == 0) {
        LOGGER.info("resultSetGroup is Empty for collection {} is {}", collection, resultSetGroup);
        this.collectionToPrevMaxDataTimeMap.remove(collection);
      } else {
        long endTime = new Double(resultSetGroup.getResultSet(0).getDouble(0)).longValue();
        this.collectionToPrevMaxDataTimeMap.put(collection, endTime);
        // endTime + 1 to make sure we cover the time range of that time value.
        String timeFormat = timeSpec.getFormat();
        if (StringUtils.isBlank(timeFormat) || TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
          maxTime = timeSpec.getDataGranularity().toMillis(endTime + 1) - 1;
        } else {
          DateTimeFormatter inputDataDateTimeFormatter =
              DateTimeFormat.forPattern(timeFormat).withZone(Utils.getDataTimeZone(collection));
          maxTime = DateTime.parse(String.valueOf(endTime), inputDataDateTimeFormatter).getMillis();
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Exception getting maxTime from collection: {}", collection, e);
      this.collectionToPrevMaxDataTimeMap.remove(collection);
    }
    if (maxTime <= 0) {
      maxTime = System.currentTimeMillis();
    }
    return maxTime;
  }

  private long getPrevMaxDataTime(String collection, TimeSpec timeSpec) {
    if (this.collectionToPrevMaxDataTimeMap.containsKey(collection)) {
      return collectionToPrevMaxDataTimeMap.get(collection);
    }
    return 0;
  }
}
