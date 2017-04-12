package com.linkedin.thirdeye.client.cache;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.pinot.PinotQuery;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class CollectionMaxDataTimeCacheLoader extends CacheLoader<String, Long> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionMaxDataTimeCacheLoader.class);
  private final static String COLLECTION_MAX_TIME_QUERY_TEMPLATE = "SELECT max(%s) FROM %s WHERE %s >= %s";

  private final LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache;
  private DatasetConfigManager datasetConfigDAO;

  private final Map<String, Long> collectionToPrevMaxDataTimeMap = new ConcurrentHashMap<String, Long>();
  private final ExecutorService reloadExecutor = Executors.newSingleThreadExecutor();

  public CollectionMaxDataTimeCacheLoader(LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache,
      DatasetConfigManager datasetConfigDAO) {
    this.resultSetGroupCache = resultSetGroupCache;
    this.datasetConfigDAO = datasetConfigDAO;
  }

  @Override
  public Long load(String collection) throws Exception {
    LOGGER.info("Loading maxDataTime cache {}", collection);
    long maxTime = 0;
    try {
      DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(collection);
      // By default, query only offline, unless dataset has been marked as realtime
      String tableName = ThirdEyeUtils.computeTableName(collection);
      TimeSpec timeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);
      long prevMaxDataTime = getPrevMaxDataTime(collection, timeSpec);
      String maxTimePql = String.format(COLLECTION_MAX_TIME_QUERY_TEMPLATE, timeSpec.getColumnName(), tableName,
          timeSpec.getColumnName(), prevMaxDataTime);
      PinotQuery maxTimePinotQuery = new PinotQuery(maxTimePql, tableName);
      resultSetGroupCache.refresh(maxTimePinotQuery);
      ResultSetGroup resultSetGroup = resultSetGroupCache.get(maxTimePinotQuery);
      if (resultSetGroup.getResultSetCount() == 0 || resultSetGroup.getResultSet(0).getRowCount() == 0) {
        LOGGER.info("resultSetGroup is Empty for collection {} is {}", tableName, resultSetGroup);
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
          DateTime endDateTime = DateTime.parse(String.valueOf(endTime), inputDataDateTimeFormatter);
          Period oneBucket = datasetConfig.bucketTimeGranularity().toPeriod();
          maxTime = endDateTime.plus(oneBucket).getMillis() - 1;
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

  @Override
  public ListenableFuture<Long> reload(final String collection, Long preMaxDataTime) {
    ListenableFutureTask<Long> reloadTask = ListenableFutureTask.create(new Callable<Long>() {
      @Override public Long call() throws Exception {
        return CollectionMaxDataTimeCacheLoader.this.load(collection);
      }
    });
    reloadExecutor.execute(reloadTask);
    LOGGER.info("Passively refreshing max data time of collection: {}", collection);
    return reloadTask;
  }

  private long getPrevMaxDataTime(String collection, TimeSpec timeSpec) {
    if (this.collectionToPrevMaxDataTimeMap.containsKey(collection)) {
      return collectionToPrevMaxDataTimeMap.get(collection);
    }
    return 0;
  }
}
