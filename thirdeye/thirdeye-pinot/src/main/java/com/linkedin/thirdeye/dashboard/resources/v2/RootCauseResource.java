package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.RCAFramework;
import com.linkedin.thirdeye.rootcause.RCAFrameworkExecutionResult;
import com.linkedin.thirdeye.rootcause.impl.DimensionEntity;
import com.linkedin.thirdeye.rootcause.impl.EntityUtils;
import com.linkedin.thirdeye.rootcause.impl.EventEntity;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.impl.ServiceEntity;
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


@Path(value = "/rootcause")
@Produces(MediaType.APPLICATION_JSON)
public class RootCauseResource {
  private static final DateTimeFormatter ISO8601 = ISODateTimeFormat.basicDateTimeNoMillis();

  private static final List<EntityFormatter> FORMATTERS = Arrays.asList(
      new MetricEntityFormatter(),
      new DimensionEntityFormatter(),
      new ServiceEntityFormatter(),
      new EventEntityFormatter(),
      new DefaultEntityFormatter()
  );

  private static final long HOUR_IN_MS = 60 * 60 * 1000;
  private static final long DAY_IN_MS = 24 * HOUR_IN_MS;

  private final RCAFramework rca;

  public RootCauseResource(RCAFramework rca) {
    this.rca = rca;
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

  static RootCauseEntity applyFormatters(Entity e) {
    for(EntityFormatter formatter : FORMATTERS) {
      if(formatter.applies(e))
        return formatter.format(e);
    }
    throw new IllegalArgumentException(String.format("No formatter for Entity '%s'", e.getUrn()));
  }

  interface EntityFormatter {
    boolean applies(Entity entity);
    RootCauseEntity format(Entity entity);
  }

  static class DefaultEntityFormatter implements EntityFormatter {
    @Override
    public boolean applies(Entity entity) {
      return true;
    }

    @Override
    public RootCauseEntity format(Entity entity) {
      String link = String.format("Entity %s", entity.getUrn());

      return makeRootCauseEntity(entity, "Other", "(none)", link);
    }
  }

  static class MetricEntityFormatter implements EntityFormatter {
    @Override
    public boolean applies(Entity entity) {
      return MetricEntity.TYPE.isType(entity.getUrn());
    }

    @Override
    public RootCauseEntity format(Entity entity) {
      MetricEntity e = MetricEntity.fromURN(entity.getUrn(), entity.getScore());

      String label = String.format("%s/%s", e.getDataset(), e.getMetric());
      String link = String.format("Metric %s", e.getUrn());

      return makeRootCauseEntity(entity, "Metric", label, link);
    }
  }

  static class DimensionEntityFormatter implements EntityFormatter {
    @Override
    public boolean applies(Entity entity) {
      return DimensionEntity.TYPE.isType(entity.getUrn());
    }

    @Override
    public RootCauseEntity format(Entity entity) {
      DimensionEntity e = DimensionEntity.fromURN(entity.getUrn(), entity.getScore());

      String label = String.format("%s='%s'", e.getName(), e.getValue());
      String link = String.format("Dimension %s", e.getUrn());

      return makeRootCauseEntity(entity, "Dimension", label, link);
    }
  }

  static class ServiceEntityFormatter implements EntityFormatter {
    @Override
    public boolean applies(Entity entity) {
      return ServiceEntity.TYPE.isType(entity.getUrn());
    }

    @Override
    public RootCauseEntity format(Entity entity) {
      ServiceEntity e = ServiceEntity.fromURN(entity.getUrn(), entity.getScore());

      String link = String.format("Service %s", e.getUrn());

      return makeRootCauseEntity(entity, "Service", e.getName(), link);
    }
  }

  static class EventEntityFormatter implements EntityFormatter {
    @Override
    public boolean applies(Entity entity) {
      return EventEntity.TYPE.isType(entity.getUrn());
    }

    @Override
    public RootCauseEntity format(Entity entity) {
      String[] parts = entity.getUrn().split(":");
      String type = parts[2];
      String name = parts[3];

      String label = String.format("%s (%s)", name, type);
      String link = String.format("Event %s", entity.getUrn());

      return makeRootCauseEntity(entity, "Event", label, link);
    }
  }

  static RootCauseEntity makeRootCauseEntity(Entity entity, String type, String label, String link) {
    RootCauseEntity out = new RootCauseEntity();
    out.setUrn(entity.getUrn());
    out.setScore(entity.getScore());
    out.setType(type);
    out.setLabel(label);
    out.setLink(link);
    return out;
  }
}
