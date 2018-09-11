/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.dashboard.resources.v2;

import com.google.common.base.Strings;
import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.TimeSeriesCompareMetricView;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.ValuesContainer;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewHandler;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewRequest;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewHandler;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewRequest;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewResponse;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Used by legacy RCA experience. To be removed.
 *
 * @see RootCauseMetricResource
 */
@Deprecated
@Path(value = "/timeseries")
@Produces(MediaType.APPLICATION_JSON)
public class TimeSeriesResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesResource.class);
  private static final String ALL = "All";

  private static final String DECIMAL_FORMAT = "%+.1f";

  private LoadingCache<String, Long> datasetMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getDatasetMaxDataTimeCache();
  private QueryCache queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();

  @GET
  @Path("/compare/{metricId}/{currentStart}/{currentEnd}/{baselineStart}/{baselineEnd}")
  public TimeSeriesCompareMetricView getTimeseriesCompareData(
      @PathParam("metricId") long metricId, @PathParam("currentStart") long currentStart,
      @PathParam("currentEnd") long currentEnd, @PathParam("baselineStart") long baselineStart,
      @PathParam("baselineEnd") long baselineEnd, @QueryParam("dimension") String dimension,
      @QueryParam("filters") String filters, @QueryParam("granularity") String granularity,
      // Auto Resize tries to limit the number of data points to 20
      @DefaultValue("false")@QueryParam("limitDataPointNum") boolean limitDataPointNum,
      @DefaultValue("20")@QueryParam("dataPointNum") int dataPointNum) {

    try {
      if (Strings.isNullOrEmpty(dimension)) {
        dimension = ALL;
      }

      MetricConfigDTO metricConfigDTO = DAO_REGISTRY.getMetricConfigDAO().findById(metricId);
      String dataset = metricConfigDTO.getDataset();
      DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
      long maxDataTime = datasetMaxDataTimeCache.get(dataset);

      currentEnd = roundEndTimeByGranularity(currentEnd, datasetConfig);
      baselineEnd = roundEndTimeByGranularity(baselineEnd, datasetConfig);
      if (currentEnd > maxDataTime) {
        long delta = currentEnd - maxDataTime;
        currentEnd = currentEnd - delta;
        baselineEnd = baselineStart + (currentEnd - currentStart);
      }

      long analysisDuration = currentEnd - currentStart;
      if (baselineEnd - baselineStart != analysisDuration) {
        baselineEnd = baselineStart + analysisDuration;
      }
      if (baselineEnd > currentEnd) {
        LOG.warn("Baseline time ranges are out of order, resetting as per current time ranges.");
        baselineEnd = currentEnd - TimeUnit.DAYS.toMillis(7);
        baselineStart = currentStart - TimeUnit.DAYS.toMillis(7);
      }

      if (StringUtils.isEmpty(granularity)) {
        granularity = "DAYS";
      }

      if (limitDataPointNum) {
        granularity = Utils.resizeTimeGranularity(analysisDuration, granularity, dataPointNum);
      }

      if (dimension.equalsIgnoreCase(ALL)) {
       return getTabularData(metricId, currentStart, currentEnd, baselineStart, baselineEnd, filters,
                granularity);
      } else {
        // build contributor view request
        return getContributorDataForDimension(metricId, currentStart, currentEnd, baselineStart,
            baselineEnd, dimension, filters, granularity);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new WebApplicationException(e);
    }
  }

  private long roundEndTimeByGranularity(long endTime, DatasetConfigDTO datasetConfig) {
    TimeGranularity bucketTimeGranularity = datasetConfig.bucketTimeGranularity();
    TimeUnit timeUnit = bucketTimeGranularity.getUnit();
    int timeSize = bucketTimeGranularity.getSize();
    DateTimeZone dateTimeZone = Utils.getDataTimeZone(datasetConfig.getDataset());
    DateTime adjustedDateTime = new DateTime(endTime, dateTimeZone);
    switch (timeUnit) {
      case DAYS:
        adjustedDateTime = adjustedDateTime.withTimeAtStartOfDay();
        break;
      case MINUTES:
        int roundedMinutes = (adjustedDateTime.getMinuteOfHour()/timeSize) * timeSize;
        adjustedDateTime = adjustedDateTime.withTime(adjustedDateTime.getHourOfDay(), roundedMinutes, 0, 0);
        break;
      case HOURS:
      default:
        adjustedDateTime = adjustedDateTime.withTime(adjustedDateTime.getHourOfDay(), 0, 0, 0);
        break;
    }
    return adjustedDateTime.getMillis();
  }

  private TimeSeriesCompareMetricView getContributorDataForDimension(long metricId,
      long currentStart, long currentEnd, long baselineStart, long baselineEnd, String dimension,
      String filters, String granularity) {

    MetricConfigDTO metricConfigDTO = DAO_REGISTRY.getMetricConfigDAO().findById(metricId);
    TimeSeriesCompareMetricView timeSeriesCompareMetricView =
        new TimeSeriesCompareMetricView(metricConfigDTO.getName(), metricId, currentStart,
            currentEnd, metricConfigDTO.isInverseMetric());

    try {
      String dataset = metricConfigDTO.getDataset();
      ContributorViewRequest request = new ContributorViewRequest();
      request.setCollection(dataset);

      MetricExpression metricExpression =
          ThirdEyeUtils.getMetricExpressionFromMetricConfig(metricConfigDTO);
      request.setMetricExpressions(Arrays.asList(metricExpression));

      DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(dataset);
      request.setBaselineStart(new DateTime(baselineStart, timeZoneForCollection));
      request.setBaselineEnd(new DateTime(baselineEnd, timeZoneForCollection));
      request.setCurrentStart(new DateTime(currentStart, timeZoneForCollection));
      request.setCurrentEnd(new DateTime(currentEnd, timeZoneForCollection));

      request.setTimeGranularity(Utils.getAggregationTimeGranularity(granularity, dataset));
      if (filters != null && !filters.isEmpty()) {
        filters = URLDecoder.decode(filters, "UTF-8");
        request.setFilters(ThirdEyeUtils.convertToMultiMap(filters));
      }

      request.setGroupByDimensions(Arrays.asList(dimension));

      ContributorViewHandler handler = new ContributorViewHandler(queryCache);
      ContributorViewResponse response = handler.process(request);

      // Assign the time buckets
      List<Long> timeBucketsCurrent = new ArrayList<>();
      List<Long> timeBucketsBaseline = new ArrayList<>();

      timeSeriesCompareMetricView.setTimeBucketsCurrent(timeBucketsCurrent);
      timeSeriesCompareMetricView.setTimeBucketsBaseline(timeBucketsBaseline);

      Map<String, ValuesContainer> subDimensionValuesMap = new LinkedHashMap<>();
      timeSeriesCompareMetricView.setSubDimensionContributionMap(subDimensionValuesMap);

      int timeBuckets = response.getTimeBuckets().size();
      // this is for over all values
      ValuesContainer vw = new ValuesContainer();
      subDimensionValuesMap.put(ALL, vw);
      vw.setCurrentValues(new double[timeBuckets]);
      vw.setBaselineValues(new double[timeBuckets]);
      vw.setPercentageChange(new String[timeBuckets]);
      vw.setCumulativeCurrentValues(new double[timeBuckets]);
      vw.setCumulativeBaselineValues(new double[timeBuckets]);
      vw.setCumulativePercentageChange(new String[timeBuckets]);

      // lets find the indices
      int subDimensionIndex =
          response.getResponseData().getSchema().getColumnsToIndexMapping().get("dimensionValue");
      int currentValueIndex =
          response.getResponseData().getSchema().getColumnsToIndexMapping().get("currentValue");
      int baselineValueIndex =
          response.getResponseData().getSchema().getColumnsToIndexMapping().get("baselineValue");
      int percentageChangeIndex =
          response.getResponseData().getSchema().getColumnsToIndexMapping().get("percentageChange");
      int cumCurrentValueIndex = response.getResponseData().getSchema().getColumnsToIndexMapping().get("cumulativeCurrentValue");
      int cumBaselineValueIndex = response.getResponseData().getSchema().getColumnsToIndexMapping().get("cumulativeBaselineValue");
      int cumPercentageChangeIndex = response.getResponseData().getSchema().getColumnsToIndexMapping().get("cumulativePercentageChange");

      // populate current and baseline time buckets
      for (int i = 0; i < timeBuckets; i++) {
        TimeBucket tb = response.getTimeBuckets().get(i);
        timeBucketsCurrent.add(tb.getCurrentStart());
        timeBucketsBaseline.add(tb.getBaselineStart());
      }

      // set current and baseline values for sub dimensions
      for (int i = 0; i < response.getResponseData().getResponseData().size(); i++) {
        String[] data = response.getResponseData().getResponseData().get(i);
        String subDimension = data[subDimensionIndex];
        Double currentVal = Double.valueOf(data[currentValueIndex]);
        Double baselineVal = Double.valueOf(data[baselineValueIndex]);
        Double percentageChangeVal = Double.valueOf(data[percentageChangeIndex]);
        Double cumCurrentVal = Double.valueOf(data[cumCurrentValueIndex]);
        Double cumBaselineVal = Double.valueOf(data[cumBaselineValueIndex]);
        Double cumPercentageChangeVal = Double.valueOf(data[cumPercentageChangeIndex]);

        int index = i % timeBuckets;

        // set overAll values
        vw.getCurrentValues()[index] += currentVal;
        vw.getBaselineValues()[index] += baselineVal;
        vw.getCumulativeCurrentValues()[index] += cumCurrentVal;
        vw.getCumulativeBaselineValues()[index] += cumBaselineVal;

        // set individual sub-dimension values
        if (!subDimensionValuesMap.containsKey(subDimension)) {
          ValuesContainer subDimVals = new ValuesContainer();
          subDimVals.setCurrentValues(new double[timeBuckets]);
          subDimVals.setBaselineValues(new double[timeBuckets]);
          subDimVals.setPercentageChange(new String[timeBuckets]);
          subDimVals.setCumulativeCurrentValues(new double[timeBuckets]);
          subDimVals.setCumulativeBaselineValues(new double[timeBuckets]);
          subDimVals.setCumulativePercentageChange(new String[timeBuckets]);
          subDimensionValuesMap.put(subDimension, subDimVals);
        }

        subDimensionValuesMap.get(subDimension).getCurrentValues()[index] = currentVal;
        subDimensionValuesMap.get(subDimension).getBaselineValues()[index] = baselineVal;
        subDimensionValuesMap.get(subDimension).getPercentageChange()[index] = String.format(DECIMAL_FORMAT, percentageChangeVal);
        subDimensionValuesMap.get(subDimension).getCumulativeCurrentValues()[index] = cumCurrentVal;
        subDimensionValuesMap.get(subDimension).getCumulativeBaselineValues()[index] = cumBaselineVal;
        subDimensionValuesMap.get(subDimension).getCumulativePercentageChange()[index] = String.format(DECIMAL_FORMAT, cumPercentageChangeVal);
      }

      // Now compute percentage change for all values
      // TODO : compute cumulative values for all
      for (int i = 0; i < vw.getCurrentValues().length; i++) {
        vw.getPercentageChange()[i] = String.format(DECIMAL_FORMAT,
            getPercentageChange(vw.getCurrentValues()[i], vw.getBaselineValues()[i]));
        vw.getCumulativePercentageChange()[i] = String.format(DECIMAL_FORMAT,
            getPercentageChange(vw.getCumulativeCurrentValues()[i],
                vw.getCumulativeBaselineValues()[i]));
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new WebApplicationException(e);
    }
    return timeSeriesCompareMetricView;
  }

  private double getPercentageChange(double current, double baseline) {
    if (baseline == 0d) {
      if (current == 0d) {
        return 0d;
      } else {
        return 100d;
      }
    } else {
      return 100 * (current - baseline) / baseline;
    }
  }

  /**
   * used when dimension is not passed, i.e. data is requested for all dimensions.
   * @param metricId
   * @param currentStart
   * @param currentEnd
   * @param baselineStart
   * @param baselineEnd
   * @param filters
   * @param granularity
   * @return
   */
  private TimeSeriesCompareMetricView getTabularData(long metricId, long currentStart, long currentEnd,
      long baselineStart, long baselineEnd, String filters, String granularity) {
    TimeSeriesCompareMetricView timeSeriesCompareView = new TimeSeriesCompareMetricView();
    try {
      MetricConfigDTO metricConfigDTO = DAO_REGISTRY.getMetricConfigDAO().findById(metricId);
      if (metricConfigDTO != null) {
        String dataset = metricConfigDTO.getDataset();

        TabularViewRequest request = new TabularViewRequest();
        request.setCollection(dataset);

        MetricExpression metricExpression =
            ThirdEyeUtils.getMetricExpressionFromMetricConfig(metricConfigDTO);
        request.setMetricExpressions(Arrays.asList(metricExpression));

        DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(dataset);
        request.setBaselineStart(new DateTime(baselineStart, timeZoneForCollection));
        request.setBaselineEnd(new DateTime(baselineEnd, timeZoneForCollection));
        request.setCurrentStart(new DateTime(currentStart, timeZoneForCollection));
        request.setCurrentEnd(new DateTime(currentEnd, timeZoneForCollection));

        request.setTimeGranularity(Utils.getAggregationTimeGranularity(granularity, dataset));
        if (filters != null && !filters.isEmpty()) {
          filters = URLDecoder.decode(filters, "UTF-8");
          request.setFilters(ThirdEyeUtils.convertToMultiMap(filters));
        }
        TabularViewHandler handler = new TabularViewHandler(queryCache);
        TabularViewResponse response = handler.process(request);

        timeSeriesCompareView.setStart(currentStart);
        timeSeriesCompareView.setEnd(currentEnd);
        timeSeriesCompareView.setMetricId(metricConfigDTO.getId());
        timeSeriesCompareView.setMetricName(metricConfigDTO.getName());
        timeSeriesCompareView.setInverseMetric(metricConfigDTO.isInverseMetric());

        List<Long> timeBucketsCurrent = new ArrayList<>();
        List<Long> timeBucketsBaseline = new ArrayList<>();

        int numTimeBuckets = response.getTimeBuckets().size();

        double [] currentValues = new double[numTimeBuckets];
        double [] baselineValues = new double[numTimeBuckets];
        String [] percentageChangeValues = new String[numTimeBuckets];

        double [] cumCurrentValues = new double[numTimeBuckets];
        double [] cumBaselineValues = new double[numTimeBuckets];
        String [] cumPercentageChangeValues = new String[numTimeBuckets];

        int currentValIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("currentValue");
        int baselineValIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("baselineValue");
        int percentageChangeIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("ratio");

        int cumCurrentValIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("cumulativeCurrentValue");
        int cumBaselineValIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("cumulativeBaselineValue");
        int cumPercentageChangeIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("cumulativeRatio");

        for (int i = 0; i < numTimeBuckets; i++) {
          TimeBucket tb = response.getTimeBuckets().get(i);
          timeBucketsCurrent.add(tb.getCurrentStart());
          timeBucketsBaseline.add(tb.getBaselineStart());

          currentValues[i] = Double.valueOf(
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(i)[currentValIndex]);
          baselineValues[i] = Double.valueOf(
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(i)[baselineValIndex]);
          percentageChangeValues[i] =
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(i)[percentageChangeIndex];

          cumCurrentValues[i] = Double.valueOf(
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(i)[cumCurrentValIndex]);
          cumBaselineValues[i] = Double.valueOf(
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(i)[cumBaselineValIndex]);
          cumPercentageChangeValues[i] =
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(i)[cumPercentageChangeIndex];
        }

        timeSeriesCompareView.setTimeBucketsCurrent(timeBucketsCurrent);
        timeSeriesCompareView.setTimeBucketsBaseline(timeBucketsBaseline);
        ValuesContainer values = new ValuesContainer();

        values.setCurrentValues(currentValues);
        values.setBaselineValues(baselineValues);
        values.setPercentageChange(percentageChangeValues);
        values.setCumulativeCurrentValues(cumCurrentValues);
        values.setCumulativeBaselineValues(cumBaselineValues);
        values.setCumulativePercentageChange(cumPercentageChangeValues);

        timeSeriesCompareView.setSubDimensionContributionMap(new LinkedHashMap<String, ValuesContainer>());
        timeSeriesCompareView.getSubDimensionContributionMap().put(ALL, values);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new WebApplicationException(e);
    }
    return timeSeriesCompareView;
  }
}
