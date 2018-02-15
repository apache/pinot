package com.linkedin.thirdeye.datasource.pinot;

import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.dashboard.Utils;

/**
 * This class helps return dimension filters for a dataset from the Pinot data source
 */
public class PinotDataSourceDimensionFilters {
  private static final Logger LOG = LoggerFactory.getLogger(PinotDataSourceDimensionFilters.class);
  private static final ExecutorService executorService = Executors.newCachedThreadPool();
  private static final int TIME_OUT_SIZE = 60;
  private static final TimeUnit TIME_OUT_UNIT = TimeUnit.SECONDS;
  private final PinotThirdEyeDataSource pinotThirdEyeDataSource;

  public PinotDataSourceDimensionFilters(PinotThirdEyeDataSource pinotThirdEyeDataSource) {
    this.pinotThirdEyeDataSource = pinotThirdEyeDataSource;
  }

  /**
   * This method gets the dimension filters for the given dataset from the pinot data source,
   * and returns them as map of dimension name to values
   * @param dataset
   * @return dimension filters map
   */
  public Map<String, List<String>> getDimensionFilters(String dataset) {
    long maxTime = System.currentTimeMillis();
    try {
      maxTime = this.pinotThirdEyeDataSource.getMaxDataTime(dataset);
    } catch (Exception e) {
      // left blank
    }

    DateTime endDateTime = new DateTime(maxTime);
    DateTime startDateTime = endDateTime.minusDays(7);

    Map<String, List<String>> filters = null;
    try {
      LOG.debug("Loading dimension filters cache {}", dataset);
      List<String> dimensions = Utils.getSortedDimensionNames(dataset);
      filters = getFilters(dataset, dimensions, startDateTime, endDateTime);
    } catch (Exception e) {
      LOG.error("Error while fetching dimension values in filter drop down for collection: {}", dataset, e);
    }
    return filters;
  }

  private Map<String, List<String>> getFilters(String dataset, List<String> dimensions, DateTime start, DateTime end) {
    DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
    MetricFunction metricFunction =
        new MetricFunction(MetricAggFunction.COUNT, "*", null, dataset, null, datasetConfig);
    List<ThirdEyeRequest> requests =
        generateFilterRequests(metricFunction, dimensions, start, end, datasetConfig.getDataSource());

    Map<ThirdEyeRequest, Future<ThirdEyeResponse>> responseFuturesMap = new LinkedHashMap<>();
    for (final ThirdEyeRequest request : requests) {
      Future<ThirdEyeResponse> responseFuture = executorService.submit(new Callable<ThirdEyeResponse>() {
        @Override
        public ThirdEyeResponse call() throws Exception {
          return pinotThirdEyeDataSource.execute(request);
        }
      });
      responseFuturesMap.put(request, responseFuture);
    }

    Map<String, List<String>> result = new HashMap<>();
    for (Map.Entry<ThirdEyeRequest, Future<ThirdEyeResponse>> entry : responseFuturesMap.entrySet()) {
      ThirdEyeRequest request = entry.getKey();
      String dimension = request.getGroupBy().get(0);
      ThirdEyeResponse thirdEyeResponse = null;
      try {
        thirdEyeResponse = entry.getValue().get(TIME_OUT_SIZE, TIME_OUT_UNIT);
      } catch (ExecutionException e) {
        LOG.error("Execution error when getting filter for Dataset '{}' in Dimension '{}'.", dataset, dimension, e);
      } catch (InterruptedException e) {
        LOG.warn("Execution is interrupted when getting filter for Dataset '{}' in Dimension '{}'.", dataset, dimension, e);
        break;
      } catch (TimeoutException e) {
        LOG.warn("Time out when getting filter for Dataset '{}' in Dimension '{}'. Time limit: {} {}", dataset, dimension,
            TIME_OUT_SIZE, TIME_OUT_UNIT);
      }
      if (thirdEyeResponse != null) {
        int n = thirdEyeResponse.getNumRowsFor(metricFunction);

        List<String> values = new ArrayList<>();
        for (int i = 0; i < n; i++) {
          Map<String, String> row = thirdEyeResponse.getRow(metricFunction, i);
          String dimensionValue = row.get(dimension);
          values.add(dimensionValue);
        }
        Collections.sort(values);
        result.put(dimension, values);
      } else {
        result.put(dimension, Collections.<String>emptyList());
      }
    }
    return result;
  }

  private static List<ThirdEyeRequest> generateFilterRequests(MetricFunction metricFunction, List<String> dimensions,
      DateTime start, DateTime end, String dataSource) {

    List<ThirdEyeRequest> requests = new ArrayList<>();

    for (String dimension : dimensions) {
      ThirdEyeRequest.ThirdEyeRequestBuilder requestBuilder = new ThirdEyeRequest.ThirdEyeRequestBuilder();
      List<MetricFunction> metricFunctions = Collections.singletonList(metricFunction);
      requestBuilder.setMetricFunctions(metricFunctions);

      requestBuilder.setStartTimeInclusive(start);
      requestBuilder.setEndTimeExclusive(end);
      requestBuilder.setGroupBy(dimension);
      requestBuilder.setDataSource(dataSource);
      ThirdEyeRequest request = requestBuilder.build("filters");
      requests.add(request);
    }

    return requests;
  }
}
