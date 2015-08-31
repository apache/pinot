package com.linkedin.thirdeye.anomaly.server.resources;

import io.dropwizard.views.View;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyDetectionConfiguration;
import com.linkedin.thirdeye.anomaly.database.DeltaTable;
import com.linkedin.thirdeye.anomaly.database.FunctionTable;
import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;
import com.linkedin.thirdeye.anomaly.generic.GenericFunctionTableRow;
import com.linkedin.thirdeye.anomaly.rulebased.RuleBasedFunctionTableRow;
import com.linkedin.thirdeye.anomaly.server.views.ActiveGenericView;
import com.linkedin.thirdeye.anomaly.server.views.ActiveRuleBasedView;
import com.linkedin.thirdeye.anomaly.server.views.AddGenericView;
import com.linkedin.thirdeye.anomaly.server.views.AddRuleBasedView;
import com.linkedin.thirdeye.anomaly.server.views.DeltaTableView;
import com.linkedin.thirdeye.anomaly.util.ResourceUtils;
import com.linkedin.thirdeye.anomaly.util.ThirdEyeServerUtils;
import com.linkedin.thirdeye.api.StarTreeConfig;

@Path("/")
@Produces(MediaType.TEXT_HTML)
public class FunctionTableResource {

  /** */
  private static final String FORM_ADD_RESPONSE_RESOURCE = "/add/post";

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionTableResource.class);

  private static final Joiner COMMA = Joiner.on(',');

  private final ThirdEyeAnomalyDetectionConfiguration config;

  public FunctionTableResource(ThirdEyeAnomalyDetectionConfiguration config) {
    this.config = config;
  }

  @GET
  public Response getRoot() {
    return Response.seeOther(URI.create("/active")).build();
  }

  @GET
  @Path("/active")
  public View getActiveView(@DefaultValue("false") @QueryParam("hideInactive") boolean hideInactive) throws Exception {
    switch (config.getMode()) {
      case GENERIC:
        return getActiveGenericView(hideInactive);
      case RULEBASED:
        return getActiveRuleBasedView(hideInactive);
      default:
        throw new IllegalStateException();
    }
  }

  @GET
  @Path("/add")
  public View getAddView() throws Exception {
    switch (config.getMode()) {
      case GENERIC:
        return new AddGenericView(config.getAnomalyDatabaseConfig().getUrl(),
            config.getAnomalyDatabaseConfig().getFunctionTableName(),
            config.getCollectionName(), FORM_ADD_RESPONSE_RESOURCE);
      case RULEBASED:
        return new AddRuleBasedView(config.getAnomalyDatabaseConfig().getUrl(),
            config.getAnomalyDatabaseConfig().getFunctionTableName(),
            config.getCollectionName(), FORM_ADD_RESPONSE_RESOURCE);
      default:
        throw new IllegalStateException();
    }
  }

  @GET
  @Path("/deactivate/{functionId}")
  public Response getDeactivateFunction(@PathParam("functionId") int functionId) throws Exception {
    try {
      String sql = String.format(ResourceUtils.getResourceAsString(
          "database/function/update-deactivate-function-template.sql"),
          config.getAnomalyDatabaseConfig().getFunctionTableName(), functionId);
      config.getAnomalyDatabaseConfig().runSQL(sql);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      return Response.serverError().entity("An error occurred:\n" + sw.toString()).build();
    }
    return Response.ok().entity(formResponseHtmlHelper("Success! Function " + functionId + " marked as inactive.", "/"))
        .build();
  }

  @GET
  @Path("/activate/{functionId}")
  public Response getActivateFunction(@PathParam("functionId") int functionId) throws Exception {
    try {
      String sql = String.format(ResourceUtils.getResourceAsString(
          "database/function/update-activate-function-template.sql"),
          config.getAnomalyDatabaseConfig().getFunctionTableName(), functionId);
      config.getAnomalyDatabaseConfig().runSQL(sql);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      return Response.serverError().entity("An error occurred:\n" + sw.toString()).build();
    }
    return Response.ok().entity(formResponseHtmlHelper("Success! Function " + functionId + " marked as active.", "/"))
        .build();
  }

  @POST
  @Path(FORM_ADD_RESPONSE_RESOURCE)
  public Response getAddPostView(MultivaluedMap<String, String> formParams) throws Exception {
    String functionName = formParams.getFirst("Name");
    String functionDescription = formParams.getFirst("Description");

    FunctionTableRow functionRow;
    switch (config.getMode()) {
      case GENERIC:
      {
        functionRow = parseGenericFunctionForm(functionName, functionDescription, formParams);
        break;
      }
      case RULEBASED:
      {
        functionRow = parseRuleBasedFunctionForm(functionName, functionDescription, formParams);
        break;
      }
      default:
        throw new IllegalStateException();
    }

    try {
      functionRow.insert(config.getAnomalyDatabaseConfig());
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      return Response.serverError().entity("An error occurred:<br><br>" + sw.toString()).build();
    }
    return Response.ok().entity(formResponseHtmlHelper("Success!", "/")).build();
  }

  @GET
  @Path("/rulebased/deltatable/{deltaTable}")
  public DeltaTableView getDeltaTableView(
      @PathParam("deltaTable") String deltaTableName) throws Exception {
    StarTreeConfig starTreeConfig = ThirdEyeServerUtils.getStarTreeConfig(config.getThirdEyeServerHost(),
        config.getThirdEyeServerPort(), config.getCollectionName());
    return new DeltaTableView(
        starTreeConfig,
        DeltaTable.load(config.getAnomalyDatabaseConfig(), starTreeConfig, deltaTableName),
        "/rulebased/deltatable/" + deltaTableName + "/post",
        config.getAnomalyDatabaseConfig().getUrl(),
        config.getAnomalyDatabaseConfig().getFunctionTableName(),
        config.getCollectionName());
  }

  @POST
  @Path("/rulebased/deltatable/{deltaTable}/post")
  public Response postDeltaTableformEntry(
      @PathParam("deltaTable") String deltaTableName,
      MultivaluedMap<String, String> formParams) throws Exception {

    List<String> columns = new ArrayList<String>(formParams.keySet());
    List<String> values = new ArrayList<String>(columns.size());
    for (String column : columns) {
      values.add("\"" + formParams.getFirst(column) + "\"");
    }
    String sql = String.format(
        ResourceUtils.getResourceAsString("database/rulebased/insert-update-delta-template.sql"),
        deltaTableName,
        COMMA.join(columns),
        COMMA.join(values));

    if (!config.getAnomalyDatabaseConfig().runSQL(sql)) {
      return Response.serverError().entity("An error occurred").build();
    }

    return Response.ok().entity(formResponseHtmlHelper("Success!", "/rulebased/deltatable/" + deltaTableName))
        .build();
  }

  private RuleBasedFunctionTableRow parseRuleBasedFunctionForm(String functionName, String functionDescription,
      MultivaluedMap<String, String> formParams) throws IOException {
    RuleBasedFunctionTableRow functionRow = new RuleBasedFunctionTableRow();
    functionRow.setFunctionName(functionName);
    functionRow.setFunctionDescription(functionDescription);
    functionRow.setCollectionName(config.getCollectionName());
    functionRow.setMetricName(formParams.getFirst("Metric"));
    functionRow.setDelta(Double.valueOf(formParams.getFirst("Delta")));
    functionRow.setAggregateUnit(TimeUnit.valueOf(formParams.getFirst("AggregateUnit")));
    functionRow.setAggregateSize(Integer.valueOf(formParams.getFirst("AggregateSize")));
    functionRow.setBaselineUnit(TimeUnit.valueOf(formParams.getFirst("BaselineUnit")));
    functionRow.setBaselineSize(Integer.valueOf(formParams.getFirst("BaselineSize")));
    functionRow.setConsecutiveBuckets(Integer.valueOf(formParams.getFirst("ConsecutiveBuckets")));
    functionRow.setCronDefinition(formParams.getFirst("CronDefinition"));
    functionRow.setDeltaTableName(formParams.getFirst("DeltaTable"));
    if (functionRow.getDeltaTableName() != null) {
      StarTreeConfig starTreeConfig = ThirdEyeServerUtils.getStarTreeConfig(config.getThirdEyeServerHost(),
          config.getThirdEyeServerPort(), config.getCollectionName());
      DeltaTable.create(config.getAnomalyDatabaseConfig(), starTreeConfig, functionRow.getDeltaTableName());
    }
    return functionRow;
  }

  private GenericFunctionTableRow parseGenericFunctionForm(String functionName, String functionDescription,
      MultivaluedMap<String, String> formParams) throws Exception {
    GenericFunctionTableRow functionRow = new GenericFunctionTableRow();
    functionRow.setFunctionName(functionName);
    functionRow.setFunctionDescription(functionDescription);
    functionRow.setCollectionName(config.getCollectionName());
    functionRow.setJarUrl(formParams.getFirst("JarUrl"));
    functionRow.setClassName(formParams.getFirst("ClassName"));
    functionRow.setFunctionProperties(formParams.getFirst("Properties"));
    return functionRow;
  }

  private ActiveGenericView getActiveGenericView(boolean hideInactive) throws Exception {
    LOGGER.info("active functions for generic");
    List<GenericFunctionTableRow> rows;
    if (hideInactive) {
      rows = FunctionTable.selectActiveRows(config.getAnomalyDatabaseConfig(), GenericFunctionTableRow.class,
          config.getCollectionName());
    } else {
      rows = FunctionTable.selectRows(config.getAnomalyDatabaseConfig(), GenericFunctionTableRow.class,
          config.getCollectionName());
    }
    return new ActiveGenericView(config.getAnomalyDatabaseConfig().getUrl(),
        config.getAnomalyDatabaseConfig().getFunctionTableName(),
        config.getCollectionName(), rows);
  }

  private ActiveRuleBasedView getActiveRuleBasedView(boolean hideInactive) throws Exception {
    LOGGER.info("active functions for rulebased");
    List<RuleBasedFunctionTableRow> rows;
    if (hideInactive) {
      rows = FunctionTable.selectActiveRows(config.getAnomalyDatabaseConfig(), RuleBasedFunctionTableRow.class,
          config.getCollectionName());
    } else {
      rows = FunctionTable.selectRows(config.getAnomalyDatabaseConfig(), RuleBasedFunctionTableRow.class,
          config.getCollectionName());
    }

    return new ActiveRuleBasedView(config.getAnomalyDatabaseConfig().getUrl(),
        config.getAnomalyDatabaseConfig().getFunctionTableName(),
        config.getCollectionName(), rows);
  }

  private static String formResponseHtmlHelper(String message, String resource) {
    return String.format("%s <a href='%s'>Return.</a>", message, resource);
  }
}
