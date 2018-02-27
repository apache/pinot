package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResourceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);
  private static final Pattern PATTERN_UNMATCHED = Pattern.compile("\\$\\{");

  private ResourceUtils() {
    // left blank
  }

  /**
   * Generates the external links (if any) for a given anomaly.
   *
   * @param mergedAnomaly anomaly dto
   * @param metricConfigDAO metric config dao
   * @return map of external urls, keyed by link label
   */
  public static Map<String, String> getExternalURLs(MergedAnomalyResultDTO mergedAnomaly, MetricConfigManager metricConfigDAO) {
    String metric = mergedAnomaly.getMetric();
    String dataset = mergedAnomaly.getCollection();
    MetricConfigDTO metricConfigDTO = metricConfigDAO.findByMetricAndDataset(metric, dataset);
    Map<String, String> urlTemplates = metricConfigDTO.getExtSourceLinkInfo();
    if (MapUtils.isEmpty(urlTemplates)) {
      return Collections.emptyMap();
    }

    // Construct context for substituting the keywords in URL template
    Map<String, String> context = new HashMap<>();
    // context for each pair of dimension name and value
    if (MapUtils.isNotEmpty(mergedAnomaly.getDimensions())) {
      for (Map.Entry<String, String> entry : mergedAnomaly.getDimensions().entrySet()) {
        putEncoded(context, entry);
      }
    }

    Map<String, String> output = new HashMap<>();
    Map<String, String> externalLinkTimeGranularity = metricConfigDTO.getExtSourceLinkTimeGranularity();
    for (Map.Entry<String, String> externalLinkEntry : urlTemplates.entrySet()) {
      String sourceName = externalLinkEntry.getKey();
      String urlTemplate = externalLinkEntry.getValue();

      if (sourceName == null || urlTemplate == null) {
        continue;
      }

      putExternalLinkTimeContext(mergedAnomaly.getStartTime(), mergedAnomaly.getEndTime(), sourceName, context, externalLinkTimeGranularity);

      StrSubstitutor strSubstitutor = new StrSubstitutor(context);
      String result = strSubstitutor.replace(urlTemplate);

      if (PATTERN_UNMATCHED.matcher(result).find()) {
        LOG.warn("Could not create valid pattern for anomaly '{}'. Skipping.", mergedAnomaly);
        continue;
      }

      output.put(sourceName, result);
    }

    return output;
  }

  /**
   * Generates the external links (if any) for a given metric slice
   *
   * @param slice metric slice
   * @param metricDAO metric config dao
   * @return map of external urls, keyed by link label
   */
  public static Map<String, String> getExternalURLs(MetricSlice slice, MetricConfigManager metricDAO) {
    MetricConfigDTO metricConfigDTO = metricDAO.findById(slice.getMetricId());
    if (metricConfigDTO == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.getMetricId()));
    }

    Map<String, String> urlTemplates = metricConfigDTO.getExtSourceLinkInfo();
    if (MapUtils.isEmpty(urlTemplates)) {
      return Collections.emptyMap();
    }

    // NOTE: this cannot handle multiple values for the same key
    Map<String, String> context = new HashMap<>();
    for (Map.Entry<String, String> entry : slice.getFilters().entries()) {
      putEncoded(context, entry);
    }

    Map<String, String> output = new HashMap<>();
    Map<String, String> externalLinkTimeGranularity = metricConfigDTO.getExtSourceLinkTimeGranularity();
    for (Map.Entry<String, String> externalLinkEntry : urlTemplates.entrySet()) {
      String sourceName = externalLinkEntry.getKey();
      String urlTemplate = externalLinkEntry.getValue();

      if (sourceName == null || urlTemplate == null) {
        continue;
      }

      putExternalLinkTimeContext(slice.getStart(), slice.getEnd(), sourceName, context, externalLinkTimeGranularity);

      StrSubstitutor strSubstitutor = new StrSubstitutor(context);
      String result = strSubstitutor.replace(urlTemplate);

      if (PATTERN_UNMATCHED.matcher(result).find()) {
        LOG.warn("Could not create valid pattern for slice '{}'. Skipping.", slice);
        continue;
      }

      output.put(sourceName, result);
    }

    return output;
  }

  /**
   * Performs URL encoding for key and value and inserts the result into the given map.
   *
   * @param context destination map
   * @param entry source entry
   */
  private static void putEncoded(Map<String, String> context, Map.Entry<String, String> entry) {
    try {
      String key = URLEncoder.encode(entry.getKey().toLowerCase(), "UTF-8");
      String value = URLEncoder.encode(entry.getValue().toLowerCase(), "UTF-8");
      context.put(key, value);
    } catch (UnsupportedEncodingException e) {
      LOG.warn("Unable to encode this dimension pair {}:{} for external links.", entry.getKey(), entry.getValue());
    }
  }

  /**
   * The default time granularity of ThirdEye is MILLISECONDS; however, it could be different for external links. This
   * method updates the start and end time according to external link's time granularity. If a link's granularity is
   * not given, then the default granularity (MILLISECONDS) is used.
   *
   * @param startTime the start time in milliseconds/
   * @param endTime the end time in milliseconds.
   * @param linkName the name of the external link.
   * @param context the context to be updated, which is used by StrSubstitutor.
   * @param externalLinkTimeGranularity the granularity of the external link.
   */
  private static void putExternalLinkTimeContext(long startTime, long endTime, String linkName,
      Map<String, String> context, Map<String, String> externalLinkTimeGranularity) {
    if (MapUtils.isNotEmpty(externalLinkTimeGranularity) && externalLinkTimeGranularity.containsKey(linkName)) {
      String timeGranularityString = externalLinkTimeGranularity.get(linkName);
      TimeGranularity timeGranularity = TimeGranularity.fromString(timeGranularityString);
      context.put(MetricConfigBean.URL_TEMPLATE_START_TIME, String.valueOf(timeGranularity.convertToUnit(startTime)));
      context.put(MetricConfigBean.URL_TEMPLATE_END_TIME, String.valueOf(timeGranularity.convertToUnit(endTime)));
    } else { // put start and end time as is
      context.put(MetricConfigBean.URL_TEMPLATE_START_TIME, String.valueOf(startTime));
      context.put(MetricConfigBean.URL_TEMPLATE_END_TIME, String.valueOf(endTime));
    }
  }
}
