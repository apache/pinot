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
import com.linkedin.thirdeye.rootcause.impl.TimeRangeEntity;

import java.util.ArrayList;
import java.util.Arrays;
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

  private static final DateTimeFormatter ISO8601 = ISODateTimeFormat.basicDateTimeNoMillis();

  private final List<RootCauseEntityFormatter> formatters;

  private static final long HOUR_IN_MS = 60 * 60 * 1000;
  private static final long DAY_IN_MS = 24 * HOUR_IN_MS;

  private final RCAFramework rca;

  public RootCauseResource(RCAFramework rca, List<RootCauseEntityFormatter> formatters) {
    this.rca = rca;
    this.formatters = formatters;
  }

  @GET
  @Path("/query")
  public List<RootCauseEntity> queryRootCause(
      @QueryParam("current") Long current,
      @QueryParam("currentDate") String currentDate,
      @QueryParam("baseline") Long baseline,
      @QueryParam("baselineDate") String baselineDate,
      @QueryParam("windowSize") Long windowSize,
      @QueryParam("windowSizeInHours") Long windowSizeInHours,
      @QueryParam("windowSizeInDays") Long windowSizeInDays,
      @QueryParam("urn") List<String> urns,
      @QueryParam("pipeline") @DefaultValue("OUTPUT") String pipeline) throws Exception {

    // input validation
    if((current == null && currentDate == null) ||
        (current != null && currentDate != null))
      throw new IllegalArgumentException("Must provide either currentDate or current");

    if((baseline == null && baselineDate == null) ||
        (baseline != null && baselineDate != null))
      throw new IllegalArgumentException("Must provide baselineDate or baseline");

    // TODO check exclusive use
    if(windowSize == null && windowSizeInHours == null && windowSizeInDays == null)
      throw new IllegalArgumentException("Must provide either windowSize, windowSizeInHours, or windowSizeInDays");

    if(currentDate != null)
      current = ISO8601.parseMillis(currentDate);

    if(baselineDate != null)
      baseline = ISO8601.parseMillis(baselineDate);

    if(windowSizeInHours != null)
      windowSize = windowSizeInHours * HOUR_IN_MS;
    if(windowSizeInDays != null)
      windowSize = windowSizeInDays * DAY_IN_MS;

    // format input
    Set<Entity> input = new HashSet<>();
    input.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_CURRENT, current - windowSize, current));
    input.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_BASELINE, baseline - windowSize, baseline));
    for(String urn : urns) {
      input.add(EntityUtils.parseURN(urn, 1.0));
    }

    // run root-cause analysis
    RCAFrameworkExecutionResult result = this.rca.run(input);

    // format output
    if(!result.getPipelineResults().containsKey(pipeline))
      throw new IllegalArgumentException(String.format("Could not find pipeline '%s'", pipeline));

    List<RootCauseEntity> output = new ArrayList<>();
    for(Entity e : result.getPipelineResults().get(pipeline).getEntities()) {
      output.add(applyFormatters(e));
    }
    Collections.sort(output, new Comparator<RootCauseEntity>() {
      @Override
      public int compare(RootCauseEntity o1, RootCauseEntity o2) {
        return -1 * Double.compare(o1.getScore(), o2.getScore());
      }
    });

    return output;
  }

  RootCauseEntity applyFormatters(Entity e) {
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
