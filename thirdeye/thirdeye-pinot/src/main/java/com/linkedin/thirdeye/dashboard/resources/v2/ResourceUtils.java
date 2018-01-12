package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResourceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);

  private ResourceUtils() {
    // left blank
  }

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
        // TODO: Change to case sensitive?
        try {
          String URLEncodedDimensionName = URLEncoder.encode(entry.getKey().toLowerCase(), "UTF-8");
          String URLEncodedDimensionValue = URLEncoder.encode(entry.getValue().toLowerCase(), "UTF-8");
          context.put(URLEncodedDimensionName, URLEncodedDimensionValue);
        } catch (UnsupportedEncodingException e) {
          LOG.warn("Unable to encode this dimension pair {}:{} for external links.", entry.getKey(), entry.getValue());
        }
      }
    }

    Long startTime = mergedAnomaly.getStartTime();
    Long endTime = mergedAnomaly.getEndTime();
    Map<String, String> externalLinkTimeGranularity = metricConfigDTO.getExtSourceLinkTimeGranularity();
    for (Map.Entry<String, String> externalLinkEntry : urlTemplates.entrySet()) {
      String sourceName = externalLinkEntry.getKey();
      String urlTemplate = externalLinkEntry.getValue();
      // context for startTime and endTime
      putExternalLinkTimeContext(startTime, endTime, sourceName, context, externalLinkTimeGranularity);

      StrSubstitutor strSubstitutor = new StrSubstitutor(context);
      String extSourceUrl = strSubstitutor.replace(urlTemplate);
      urlTemplates.put(sourceName, extSourceUrl);
    }

    return urlTemplates;
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
