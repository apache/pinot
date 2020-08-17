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

package org.apache.pinot.thirdeye.dashboard.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Singleton;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.cube.additive.AdditiveDBClient;
import org.apache.pinot.thirdeye.cube.cost.BalancedCostFunction;
import org.apache.pinot.thirdeye.cube.cost.CostFunction;
import org.apache.pinot.thirdeye.cube.cost.RatioCostFunction;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.entry.MultiDimensionalRatioSummary;
import org.apache.pinot.thirdeye.cube.entry.MultiDimensionalSummary;
import org.apache.pinot.thirdeye.cube.entry.MultiDimensionalSummaryCLITool;
import org.apache.pinot.thirdeye.cube.ratio.RatioDBClient;
import org.apache.pinot.thirdeye.cube.summary.SummaryResponse;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.common.constants.rca.MultiDimensionalSummaryConstants.*;
import static org.apache.pinot.thirdeye.common.constants.rca.RootCauseResourceConstants.*;


@Singleton
public class SummaryResource {
  private static final Logger LOG = LoggerFactory.getLogger(SummaryResource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final MetricConfigManager metricConfigDAO = DAORegistry.getInstance().getMetricConfigDAO();
  private static final DatasetConfigManager datasetConfigDAO = DAORegistry.getInstance().getDatasetConfigDAO();

  public static final String DEFAULT_TIMEZONE_ID = "UTC";
  public static final String DEFAULT_DEPTH = "3";
  public static final String DEFAULT_HIERARCHIES = "[]";
  public static final String DEFAULT_ONE_SIDE_ERROR = "false";
  public static final String DEFAULT_EXCLUDED_DIMENSIONS = "";
  private static final String JAVASCRIPT_NULL_STRING = "undefined";
  private static final String HTML_STRING_ENCODING = "UTF-8";

  private static final String NUMERATOR_GROUP_NAME = "numerator";
  private static final String DENOMINATOR_GROUP_NAME = "denominator";
  // Only match string like "id123/id456" but not "id123*100/id456"
  // The 1st metric id will be put into numerator group and the 2nd metric id will be in denominator group
  private static final String SIMPLE_RATIO_METRIC_EXPRESSION_ID_PARSER =
      "^id(?<" + NUMERATOR_GROUP_NAME + ">\\d*)\\/id(?<" + DENOMINATOR_GROUP_NAME + ">\\d*)$";

  private MetricConfigDTO fetchMetricConfig(String metricUrn, String metric, String dataset) {
    MetricConfigDTO metricConfigDTO;
    if (StringUtils.isNotBlank(metricUrn)) {
      metricConfigDTO = metricConfigDAO.findById(MetricEntity.fromURN(metricUrn).getId());
    } else {
      metricConfigDTO = metricConfigDAO.findByMetricAndDataset(metric, dataset);
    }
    return metricConfigDTO;
  }

  private SummaryResponse buildDataCubeSummary(String metricUrn, String dataset, String metric, long currentStartInclusive,
      long currentEndExclusive, long baselineStartInclusive, long baselineEndExclusive, String groupByDimensions,
      String filterJsonPayload, int summarySize, int depth, String hierarchiesPayload, boolean doOneSideError,
      String excludedDimensions, String timeZone) throws Exception {
    if (summarySize < 1) summarySize = 1;

    String metricName = metric;
    String datasetName = dataset;
    SummaryResponse response = null;
    try {
      MetricConfigDTO metricConfigDTO = fetchMetricConfig(metricUrn, metric, dataset);
      if (metricConfigDTO != null) {
        metricName = metricConfigDTO.getName();
        datasetName = metricConfigDTO.getDataset();
      }

      Dimensions dimensions;
      if (StringUtils.isBlank(groupByDimensions) || JAVASCRIPT_NULL_STRING.equals(groupByDimensions)) {
        DatasetConfigDTO datasetConfigDTO = datasetConfigDAO.findByDataset(dataset);
        List<String> dimensionNames = new ArrayList<>();
        if (datasetConfigDTO != null) {
          dimensionNames = datasetConfigDTO.getDimensions();
        }
        dimensions = MultiDimensionalSummaryCLITool.sanitizeDimensions(new Dimensions(dimensionNames));
      } else {
        dimensions = new Dimensions(Arrays.asList(groupByDimensions.trim().split(",")));
      }

      if (!Strings.isNullOrEmpty(excludedDimensions)) {
        List<String> dimensionsToBeRemoved = Arrays.asList(excludedDimensions.trim().split(","));
        dimensions = MultiDimensionalSummaryCLITool.removeDimensions(dimensions, dimensionsToBeRemoved);
      }

      Multimap<String, String> filterSetMap;
      if (StringUtils.isBlank(filterJsonPayload) || JAVASCRIPT_NULL_STRING.equals(filterJsonPayload)) {
        filterSetMap = ArrayListMultimap.create();
      } else {
        filterJsonPayload = URLDecoder.decode(filterJsonPayload, HTML_STRING_ENCODING);
        filterSetMap = ThirdEyeUtils.convertToMultiMap(filterJsonPayload);
      }

      List<List<String>> hierarchies = OBJECT_MAPPER.readValue(hierarchiesPayload,
          new TypeReference<List<List<String>>>() {});

      DateTimeZone dateTimeZone = DateTimeZone.forID(timeZone);

      // Non simple ratio metrics
      if (!isSimpleRatioMetric(metricConfigDTO)) {
        response =
            runAdditiveCubeAlgorithm(dateTimeZone, datasetName, metricName, currentStartInclusive,
                currentEndExclusive, baselineStartInclusive, baselineEndExclusive, dimensions, filterSetMap,
                summarySize, depth, hierarchies, doOneSideError);
      } else {  // Simple ratio metric such as "A/B". On the contrary, "A*100/B" is not a simple ratio metric.
        response =
            runRatioCubeAlgorithm(dateTimeZone, datasetName, metricConfigDTO, currentStartInclusive,
                currentEndExclusive, baselineStartInclusive, baselineEndExclusive, dimensions, filterSetMap,
                summarySize, depth, hierarchies, doOneSideError);
      }
    } catch (Exception e) {
      LOG.error("Exception while generating difference summary", e);
      if (metricUrn != null) {
        response = SummaryResponse.buildNotAvailableResponse(metricUrn);
      } else {
        response = SummaryResponse.buildNotAvailableResponse(datasetName, metricName);
      }
    }

    return response;
  }

  @GET
  @Path("autoDimensionOrder/v2")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, Object> getDataCubeSummary(
      @QueryParam(METRIC_URN) String metricUrn,
      @QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric,
      @QueryParam(CURRENT_START) long currentStartInclusive,
      @QueryParam(CURRENT_END) long currentEndExclusive,
      @QueryParam(BASELINE_START) long baselineStartInclusive,
      @QueryParam(BASELINE_END) long baselineEndExclusive,
      @QueryParam("dimensions") String groupByDimensions,
      @QueryParam("filters") String filterJsonPayload,
      @QueryParam(CUBE_SUMMARY_SIZE) int summarySize,
      @QueryParam(CUBE_DEPTH) @DefaultValue(DEFAULT_DEPTH) int depth,
      @QueryParam(CUBE_DIM_HIERARCHIES) @DefaultValue(DEFAULT_HIERARCHIES) String hierarchiesPayload,
      @QueryParam(CUBE_ONE_SIDE_ERROR) @DefaultValue(DEFAULT_ONE_SIDE_ERROR) boolean doOneSideError,
      @QueryParam(CUBE_EXCLUDED_DIMENSIONS) @DefaultValue(DEFAULT_EXCLUDED_DIMENSIONS) String excludedDimensions,
      @QueryParam(TIME_ZONE) @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone) throws Exception {
    SummaryResponse response = buildDataCubeSummary(metricUrn, metric, dataset, currentStartInclusive, currentEndExclusive, baselineStartInclusive,
        baselineEndExclusive, groupByDimensions, filterJsonPayload, summarySize, depth, hierarchiesPayload,
        doOneSideError, excludedDimensions, timeZone);
    return OBJECT_MAPPER.convertValue(response, Map.class);
  }

  @GET
  @Path("autoDimensionOrder")
  @Produces(MediaType.APPLICATION_JSON)
  public String buildSummary(
      @QueryParam(METRIC_URN) String metricUrn,
      @QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric,
      @QueryParam(CURRENT_START) long currentStartInclusive,
      @QueryParam(CURRENT_END) long currentEndExclusive,
      @QueryParam(BASELINE_START) long baselineStartInclusive,
      @QueryParam(BASELINE_END) long baselineEndExclusive,
      @QueryParam("dimensions") String groupByDimensions,
      @QueryParam("filters") String filterJsonPayload,
      @QueryParam(CUBE_SUMMARY_SIZE) int summarySize,
      @QueryParam(CUBE_DEPTH) @DefaultValue(DEFAULT_DEPTH) int depth,
      @QueryParam(CUBE_DIM_HIERARCHIES) @DefaultValue(DEFAULT_HIERARCHIES) String hierarchiesPayload,
      @QueryParam(CUBE_ONE_SIDE_ERROR) @DefaultValue(DEFAULT_ONE_SIDE_ERROR) boolean doOneSideError,
      @QueryParam(CUBE_EXCLUDED_DIMENSIONS) @DefaultValue(DEFAULT_EXCLUDED_DIMENSIONS) String excludedDimensions,
      @QueryParam(TIME_ZONE) @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone) throws Exception {
    SummaryResponse response = buildDataCubeSummary(metricUrn, metric, dataset, currentStartInclusive, currentEndExclusive, baselineStartInclusive,
        baselineEndExclusive, groupByDimensions, filterJsonPayload, summarySize, depth, hierarchiesPayload,
        doOneSideError, excludedDimensions, timeZone);
    return OBJECT_MAPPER.writeValueAsString(response);
  }

  @GET
  @Path("manualDimensionOrder")
  @Produces(MediaType.APPLICATION_JSON)
  public String buildSummaryManualDimensionOrder(
      @QueryParam(METRIC_URN) String metricUrn,
      @QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric,
      @QueryParam(CURRENT_START) long currentStartInclusive,
      @QueryParam(CURRENT_END) long currentEndExclusive,
      @QueryParam(BASELINE_START) long baselineStartInclusive,
      @QueryParam(BASELINE_END) long baselineEndExclusive,
      @QueryParam("dimensions") String groupByDimensions,
      @QueryParam("filters") String filterJsonPayload,
      @QueryParam(CUBE_SUMMARY_SIZE) int summarySize,
      @QueryParam(CUBE_ONE_SIDE_ERROR) @DefaultValue(DEFAULT_ONE_SIDE_ERROR) boolean doOneSideError,
      @QueryParam(TIME_ZONE) @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone) throws Exception {
    if (summarySize < 1) summarySize = 1;

    String metricName = metric;
    String datasetName = dataset;
    SummaryResponse response = null;
    try {
      MetricConfigDTO metricConfigDTO = fetchMetricConfig(metricUrn, metric, dataset);
      if (metricConfigDTO != null) {
        metricName = metricConfigDTO.getName();
        datasetName = metricConfigDTO.getDataset();
      }

      List<String> allDimensions = new ArrayList<>();
      if (StringUtils.isBlank(groupByDimensions) || JAVASCRIPT_NULL_STRING.equals(groupByDimensions)) {
        DatasetConfigDTO datasetConfigDTO = datasetConfigDAO.findByDataset(dataset);
        if (datasetConfigDTO != null) {
          allDimensions = datasetConfigDTO.getDimensions();
        }
      } else {
        allDimensions = Arrays.asList(groupByDimensions.trim().split(","));
      }
      if (allDimensions.size() > Integer.parseInt(DEFAULT_DEPTH)) {
        allDimensions = allDimensions.subList(0, Integer.parseInt(DEFAULT_DEPTH));
      }
      Dimensions dimensions = new Dimensions(allDimensions);

      Multimap<String, String> filterSets;
      if (StringUtils.isBlank(filterJsonPayload) || JAVASCRIPT_NULL_STRING.equals(filterJsonPayload)) {
        filterSets = ArrayListMultimap.create();
      } else {
        filterJsonPayload = URLDecoder.decode(filterJsonPayload, HTML_STRING_ENCODING);
        filterSets = ThirdEyeUtils.convertToMultiMap(filterJsonPayload);
      }

      DateTimeZone dateTimeZone = DateTimeZone.forID(timeZone);

      // Non simple ratio metrics
      if (!isSimpleRatioMetric(metricConfigDTO)) {
        response =
            runAdditiveCubeAlgorithm(dateTimeZone, datasetName, metricName, currentStartInclusive,
                currentEndExclusive, baselineStartInclusive, baselineEndExclusive, dimensions, filterSets,
                summarySize, 0, Collections.emptyList(), doOneSideError);
      } else {  // Simple ratio metric such as "A/B". On the contrary, "A*100/B" is not a simple ratio metric.
        response =
            runRatioCubeAlgorithm(dateTimeZone, datasetName, metricConfigDTO, currentStartInclusive,
                currentEndExclusive, baselineStartInclusive, baselineEndExclusive, dimensions, filterSets,
                summarySize, 0, Collections.emptyList(), doOneSideError);
      }
    } catch (Exception e) {
      LOG.error("Exception while generating difference summary", e);
      if (metricUrn != null) {
        response = SummaryResponse.buildNotAvailableResponse(metricUrn);
      } else {
        response = SummaryResponse.buildNotAvailableResponse(datasetName, metricName);
      }
    }
    return OBJECT_MAPPER.writeValueAsString(response);
  }

  /**
   * Executes cube algorithm for the given additive metric.
   *
   * @param dateTimeZone time zone of the data.
   * @param dataset dataset name.
   * @param metric metric name.
   * @param currentStartInclusive timestamp of current start.
   * @param currentEndExclusive timestamp of current end.
   * @param baselineStartInclusive timestamp of baseline start.
   * @param baselineEndExclusive timestamp of baseline end.
   * @param dimensions ordered dimensions to be drilled down by the algorithm.
   * @param dataFilters the filter to be applied on the data. Thus, the algorithm will only analyze a subset of data.
   * @param summarySize the size of the summary result.
   * @param depth the depth of the dimensions to be analyzed.
   * @param hierarchies the hierarchy among the dimensions.
   * @param doOneSideError flag to toggle if we only want one side results.
   *
   * @return the summary result of cube algorithm.
   */
  private SummaryResponse runAdditiveCubeAlgorithm(DateTimeZone dateTimeZone, String dataset, String metric,
      long currentStartInclusive, long currentEndExclusive, long baselineStartInclusive, long baselineEndExclusive,
      Dimensions dimensions, Multimap<String, String> dataFilters, int summarySize, int depth,
      List<List<String>> hierarchies, boolean doOneSideError) throws Exception {

    CostFunction costFunction = new BalancedCostFunction();
    AdditiveDBClient cubeDbClient = new AdditiveDBClient(CACHE_REGISTRY_INSTANCE.getQueryCache());
    MultiDimensionalSummary mdSummary = new MultiDimensionalSummary(cubeDbClient, costFunction, dateTimeZone);

    return mdSummary.buildSummary(dataset, metric, currentStartInclusive, currentEndExclusive, baselineStartInclusive,
        baselineEndExclusive, dimensions, dataFilters, summarySize, depth, hierarchies, doOneSideError);
  }

  /**
   * Executes cube algorithm for the given ratio metric.
   *
   * @param dateTimeZone time zone of the data.
   * @param dataset dataset name.
   * @param metricConfigDTO the metric config of the ratio metric.
   * @param currentStartInclusive timestamp of current start.
   * @param currentEndExclusive timestamp of current end.
   * @param baselineStartInclusive timestamp of baseline start.
   * @param baselineEndExclusive timestamp of baseline end.
   * @param dimensions ordered dimensions to be drilled down by the algorithm.
   * @param dataFilters the filter to be applied on the data. Thus, the algorithm will only analyze a subset of data.
   * @param summarySize the size of the summary result.
   * @param depth the depth of the dimensions to be analyzed.
   * @param hierarchies the hierarchy among the dimensions.
   * @param doOneSideError flag to toggle if we only want one side results.
   *
   * @return the summary result of cube algorithm.
   */
  private SummaryResponse runRatioCubeAlgorithm(DateTimeZone dateTimeZone, String dataset,
      MetricConfigDTO metricConfigDTO, long currentStartInclusive, long currentEndExclusive,
      long baselineStartInclusive, long baselineEndExclusive, Dimensions dimensions,
      Multimap<String, String> dataFilters, int summarySize, int depth, List<List<String>> hierarchies,
      boolean doOneSideError) throws Exception {
    Preconditions.checkNotNull(metricConfigDTO);

    // Construct regular expression parser
    String derivedMetricExpression = metricConfigDTO.getDerivedMetricExpression();
    MatchedRatioMetricsResult matchedRatioMetricsResult = parseNumeratorDenominatorId(derivedMetricExpression);

    if (matchedRatioMetricsResult.hasFound) {
      // Extract numerator and denominator id
      long numeratorId = matchedRatioMetricsResult.numeratorId;
      long denominatorId = matchedRatioMetricsResult.denominatorId;
      // Get numerator and denominator's metric name
      String numeratorMetric = metricConfigDAO.findById(numeratorId).getName();
      String denominatorMetric = metricConfigDAO.findById(denominatorId).getName();
      // Generate cube result
      CostFunction costFunction = new RatioCostFunction();
      RatioDBClient dbClient = new RatioDBClient(CACHE_REGISTRY_INSTANCE.getQueryCache());
      MultiDimensionalRatioSummary mdSummary = new MultiDimensionalRatioSummary(dbClient, costFunction, dateTimeZone);

      return mdSummary.buildRatioSummary(dataset, numeratorMetric, denominatorMetric, currentStartInclusive,
          currentEndExclusive, baselineStartInclusive, baselineEndExclusive, dimensions, dataFilters, summarySize,
          depth, hierarchies, doOneSideError);
    } else { // parser should find ids because of the guard of the if-condition.
      LOG.error("Unable to parser numerator and denominator metric for metric" + metricConfigDTO.getName());
      return SummaryResponse.buildNotAvailableResponse(dataset, metricConfigDTO.getName());
    }
  }

  /**
   * Returns if the given metric is a simple ratio metric such as "A/B" where A and B is a metric.
   *
   * @param metricConfigDTO the config of a metric.
   *
   * @return true if the given metric is a simple ratio metric.
   */
  static boolean isSimpleRatioMetric(MetricConfigDTO metricConfigDTO) {
    if (metricConfigDTO != null) {
      String metricExpression = metricConfigDTO.getDerivedMetricExpression();
      if (!Strings.isNullOrEmpty(metricExpression)) {
        Pattern pattern = Pattern.compile(SIMPLE_RATIO_METRIC_EXPRESSION_ID_PARSER);
        Matcher matcher = pattern.matcher(metricExpression);
        return matcher.matches();
      }
    }
    return false;
  }

  /**
   * Parse numerator and denominator id from a given metric expression string.
   *
   * @param derivedMetricExpression the given metric expression.
   *
   * @return the parsed result, which is stored in MatchedRatioMetricsResult.
   */
  static MatchedRatioMetricsResult parseNumeratorDenominatorId(String derivedMetricExpression) {
    if (Strings.isNullOrEmpty(derivedMetricExpression)) {
      return new MatchedRatioMetricsResult(false, -1, -1);
    }

    Pattern pattern = Pattern.compile(SIMPLE_RATIO_METRIC_EXPRESSION_ID_PARSER);
    Matcher matcher = pattern.matcher(derivedMetricExpression);
    if (matcher.find()) {
      // Extract numerator and denominator id
      long numeratorId = Long.valueOf(matcher.group(NUMERATOR_GROUP_NAME));
      long denominatorId = Long.valueOf(matcher.group(DENOMINATOR_GROUP_NAME));
      return new MatchedRatioMetricsResult(true, numeratorId, denominatorId);
    } else {
      return new MatchedRatioMetricsResult(false, -1, -1);
    }
  }

  /**
   * The class to store the parsed numerator and denominator id.
   */
  static class MatchedRatioMetricsResult {
    boolean hasFound; // false is parsing is failed.
    long numeratorId; // numerator id if parsing is succeeded.
    long denominatorId; // denominator id if parsing is succeeded.

    /**
     * Construct the object that stores the parsed numerator and denominator id.
     * @param hasFound false is parsing is failed.
     * @param numeratorId numerator id if parsing is succeeded.
     * @param denominatorId denominator id if parsing is succeeded.
     */
    MatchedRatioMetricsResult(boolean hasFound, long numeratorId, long denominatorId) {
      this.hasFound = hasFound;
      this.numeratorId = numeratorId;
      this.denominatorId = denominatorId;
    }
  }
}
