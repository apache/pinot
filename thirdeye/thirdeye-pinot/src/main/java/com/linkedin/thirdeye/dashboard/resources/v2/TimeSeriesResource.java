package com.linkedin.thirdeye.dashboard.resources.v2;

import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.TimeSeriesCompareMetricView;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewHandler;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewRequest;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewResponse;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

@Path(value = "/timeseries")
@Produces(MediaType.APPLICATION_JSON)
public class TimeSeriesResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesResource.class);

  private LoadingCache<String, Long> datasetMaxDataTimeCache = CACHE_REGISTRY_INSTANCE
      .getCollectionMaxDataTimeCache();
  private QueryCache queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
  private MetricConfigManager metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();

  @GET
  @Path("/compare/{metricId}/{currentStart}/{currentEnd}/{baselineStart}/{baselineEnd}")
  public TimeSeriesCompareMetricView getTimeseriesCompareData(
      @PathParam("metricId") long metricId, @PathParam("currentStart") long currentStart,
      @PathParam("currentEnd") long currentEnd, @PathParam("baselineStart") long baselineStart,
      @PathParam("baselineEnd") long baselineEnd, @QueryParam("dimensions") String dimensions,
      @QueryParam("filters") String filters, @QueryParam("granularity") String granularity) {

    TimeSeriesCompareMetricView timeSeriesCompareView = new TimeSeriesCompareMetricView();
    try {
      MetricConfigDTO metricConfigDTO = metricConfigDAO.findById(metricId);
      if (metricConfigDTO != null) {
        String dataset = metricConfigDTO.getDataset();

        TabularViewRequest request = new TabularViewRequest();
        request.setCollection(dataset);

        MetricExpression metricExpression =
            ThirdEyeUtils.getMetricExpressionFromMetricConfig(metricConfigDTO);
        request.setMetricExpressions(Arrays.asList(metricExpression));

        long maxDataTime = datasetMaxDataTimeCache.get(dataset);
        if (currentEnd > maxDataTime) {
          long delta = currentEnd - maxDataTime;
          currentEnd = currentEnd - delta;
          baselineEnd = baselineStart + (currentEnd - currentStart);
        }
        DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(dataset);
        request.setBaselineStart(new DateTime(baselineStart, timeZoneForCollection));
        request.setBaselineEnd(new DateTime(baselineEnd, timeZoneForCollection));
        request.setCurrentStart(new DateTime(currentStart, timeZoneForCollection));
        request.setCurrentEnd(new DateTime(currentEnd, timeZoneForCollection));

        if (StringUtils.isEmpty(granularity)) {
          granularity = "DAYS";
        }
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

        List<Long> timeBucketsCurrent = new ArrayList<>();
        List<Long> timeBucketsBaseline = new ArrayList<>();
        List<Double> currentValues = new ArrayList<>();
        List<Double> baselineValues = new ArrayList<>();
        int currentValIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("currentValue");
        int baselineValIndex =
            response.getData().get(metricConfigDTO.getName()).getSchema().getColumnsToIndexMapping()
                .get("baselineValue");
        int count = 0;
        for (TimeBucket tb : response.getTimeBuckets()) {
          timeBucketsCurrent.add(tb.getCurrentStart());
          timeBucketsBaseline.add(tb.getBaselineStart());
          currentValues.add(Double.valueOf(
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(count)[currentValIndex]));
          baselineValues.add(Double.valueOf(
              response.getData().get(metricConfigDTO.getName()).getResponseData()
                  .get(count)[baselineValIndex]));
          count++;
        }

        timeSeriesCompareView.setTimeBucketsCurrent(timeBucketsCurrent);
        timeSeriesCompareView.setTimeBucketsBaseline(timeBucketsBaseline);
        timeSeriesCompareView.setCurrentValues(currentValues);
        timeSeriesCompareView.setBaselineValues(baselineValues);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new WebApplicationException(e);
    }
    return timeSeriesCompareView;
  }
}
