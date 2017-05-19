package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.dashboard.resources.v2.rootcause.DefaultEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.rootcause.DimensionEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.rootcause.EventEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.rootcause.MetricEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.rootcause.ServiceEntityFormatter;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.RCAFramework;
import com.linkedin.thirdeye.rootcause.RCAFrameworkExecutionResult;
import com.linkedin.thirdeye.rootcause.impl.EntityUtils;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.impl.TimeRangeEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path(value = "/rootcause")
@Produces(MediaType.APPLICATION_JSON)
public class RootCauseResource {
  private static final Logger LOG = LoggerFactory.getLogger(RootCauseResource.class);

  private final List<RootCauseEntityFormatter> formatters;

  private final RCAFramework rootCauseFramework;
  private final RCAFramework relatedMetricsFramework;

  public RootCauseResource(RCAFramework rootCauseFramework, RCAFramework relatedMetricsFramework, List<RootCauseEntityFormatter> formatters) {
    this.rootCauseFramework = rootCauseFramework;
    this.relatedMetricsFramework = relatedMetricsFramework;
    this.formatters = formatters;

    if(this.rootCauseFramework == null)
      LOG.info("RootCauseFramework not configured. Disabling '/queryRootCause' endpoint.");

    if(this.relatedMetricsFramework == null)
      LOG.info("RelatedMetricsFramework not configured. Disabling '/queryRelatedMetrics' endpoint.");
  }

  @GET
  @Path("/queryRootCause")
  public List<RootCauseEntity> queryRootCause(
      @QueryParam("current") Long current,
      @QueryParam("baseline") Long baseline,
      @QueryParam("windowSize") Long windowSize,
      @QueryParam("urn") List<String> urns) throws Exception {

    // configuration validation
    if(this.rootCauseFramework == null)
      throw new IllegalStateException("RootCauseFramework not configured. Endpoint disabled.");

    // input validation
    if(current == null)
      throw new IllegalArgumentException("Must provide current timestamp (in milliseconds)");

    if(baseline == null)
      throw new IllegalArgumentException("Must provide baseline timestamp (in milliseconds)");

    if(windowSize == null)
      throw new IllegalArgumentException("Must provide windowSize (in milliseconds)");

    // format input
    Set<Entity> input = new HashSet<>();
    input.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_CURRENT, current - windowSize, current));
    input.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_BASELINE, baseline - windowSize, baseline));
    for(String urn : urns) {
      input.add(EntityUtils.parseURN(urn, 1.0));
    }

    // run root-cause analysis
    RCAFrameworkExecutionResult result = this.rootCauseFramework.run(input);

    // apply formatters
    return applyFormatters(result.getResultsSorted());
  }

  @GET
  @Path("/queryRelatedMetrics")
  public List<RootCauseEntity> queryRelatedMetrics(
      @QueryParam("current") Long current,
      @QueryParam("baseline") Long baseline,
      @QueryParam("windowSize") Long windowSize,
      @QueryParam("metricUrn") String metricUrn) throws Exception {

    // configuration validation
    if(this.relatedMetricsFramework == null)
      throw new IllegalStateException("RelatedMetricsFramework not configured. Endpoint disabled.");

    // input validation
    if(current == null)
      throw new IllegalArgumentException("Must provide current timestamp (in milliseconds)");

    if(baseline == null)
      throw new IllegalArgumentException("Must provide baseline timestamp (in milliseconds)");

    if(windowSize == null)
      throw new IllegalArgumentException("Must provide windowSize (in milliseconds)");

    if(metricUrn == null)
      throw new IllegalArgumentException("Must provide metricUrn");

    if(!MetricEntity.TYPE.isType(metricUrn))
      throw new IllegalArgumentException(String.format("URN '%s' is not a MetricEntity", metricUrn));

    // format input
    Set<Entity> input = new HashSet<>();
    input.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_CURRENT, current - windowSize, current));
    input.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_BASELINE, baseline - windowSize, baseline));
    input.add(EntityUtils.parseURN(metricUrn, 1.0));

    // run related metrics analysis
    RCAFrameworkExecutionResult result = this.relatedMetricsFramework.run(input);

    // apply formatters
    return applyFormatters(result.getResultsSorted());
  }

  private List<RootCauseEntity> applyFormatters(Iterable<Entity> entities) {
    List<RootCauseEntity> output = new ArrayList<>();
    for(Entity e : entities) {
      output.add(applyFormatters(e));
    }
    return output;
  }

  private RootCauseEntity applyFormatters(Entity e) {
    for(RootCauseEntityFormatter formatter : this.formatters) {
      if(formatter.applies(e)) {
        try {
          return formatter.format(e);
        } catch (Exception ex) {
          LOG.warn("Error applying formatter '{}'. Skipping.", formatter.getClass().getName(), ex);
        }
      }
    }
    throw new IllegalArgumentException(String.format("No formatter for Entity '%s'", e.getUrn()));
  }

}
