package org.apache.pinot.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.broker.routing.RoutingTable;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class QueryEnvironmentTest {
  private static org.apache.pinot.spi.data.Schema SCHEMA;

  static {
    SCHEMA =
        new org.apache.pinot.spi.data.Schema.SchemaBuilder().setSchemaName("schema")
            .addSingleValueDimension("c1", FieldSpec.DataType.STRING, "")
            .addSingleValueDimension("c2", FieldSpec.DataType.STRING, "")
            .addMetric("c3", FieldSpec.DataType.INT, 0).build();
  }
  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp() {
    RoutingManager routingManager = getMockRoutingManager();
    _queryEnvironment = new QueryEnvironment(
        new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(mockTableCache())),
        new WorkerManager(routingManager)
    );
  }

  @Test
  public void testSqlStrings() throws Exception {
    testQueryParsing("SELECT * FROM a JOIN b ON a.c1 = b.c2 WHERE a.c3 >= 0",
        "SELECT *\n" + "FROM `a`\n" + "INNER JOIN `b` ON `a`.`c1` = `b`.`c2`\n" + "WHERE `a`.`c3` >= 0");
  }

  @Test
  public void testQueryToStages()
      throws Exception {
    PlannerContext plannerContext = new PlannerContext();
    String query = "SELECT * FROM a JOIN b ON a.c1 = b.c2 WHERE a.c3 >= 0";
    QueryPlan queryPlan = _queryEnvironment.sqlQuery(query);
    Assert.assertEquals(queryPlan.getQueryStageMap().size(), 3);
    Assert.assertEquals(queryPlan.getStageMetadataMap().size(), 3);
    for (StageMetadata stageMetadata : queryPlan.getStageMetadataMap().values()) {
      List<String> tables = stageMetadata.getScannedTables();
      if (tables.size() == 1) {
        Assert.assertEquals(
            stageMetadata.getServerInstances().stream().map(ServerInstance::toString).collect(Collectors.toList()),
            tables.get(0).equals("a") ? List.of("Server_localhost_1", "Server_localhost_2")
                : List.of("Server_localhost_2"));
      } else {
        Assert.assertEquals(
            stageMetadata.getServerInstances().stream().map(ServerInstance::toString).collect(Collectors.toList()),
            List.of("Server_localhost_1", "Server_localhost_2"));
      }
    }
  }

  @Test
  public void testQueryToRel()
      throws Exception {
    PlannerContext plannerContext = new PlannerContext();
    String query = "SELECT * FROM a JOIN b ON a.c1 = b.c2 WHERE a.c3 >= 0";
    SqlNode parsed = _queryEnvironment.parse(query, plannerContext);
    SqlNode validated = _queryEnvironment.validate(parsed);
    RelRoot relRoot = _queryEnvironment.toRelation(validated, plannerContext);
    RelNode optimized = _queryEnvironment.optimize(relRoot, plannerContext);
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    RelWriter planWriter = new RelXmlWriter(pw, SqlExplainLevel.ALL_ATTRIBUTES);
    optimized.explain(planWriter);
    String xmlEncodedPlan = sw.toString();
    // TODO: do XML validation.
    Assert.assertEquals(xmlEncodedPlan,
        "<RelNode type=\"LogicalJoin\">\n" + "\t<Property name=\"condition\">\n" + "\t\t=($1, $5)\t</Property>\n"
            + "\t<Property name=\"joinType\">\n" + "\t\tinner\t</Property>\n" + "\t<Inputs>\n"
            + "\t\t<RelNode type=\"LogicalExchange\">\n" + "\t\t\t<Property name=\"distribution\">\n"
            + "\t\t\t\tsingle\t\t\t</Property>\n" + "\t\t\t<Inputs>\n" + "\t\t\t\t<RelNode type=\"LogicalCalc\">\n"
            + "\t\t\t\t\t<Property name=\"expr#0..2\">\n" + "\t\t\t\t\t\t{inputs}\t\t\t\t\t</Property>\n"
            + "\t\t\t\t\t<Property name=\"expr#3\">\n" + "\t\t\t\t\t\t0\t\t\t\t\t</Property>\n"
            + "\t\t\t\t\t<Property name=\"expr#4\">\n" + "\t\t\t\t\t\t&#62;=($t0, $t3)\t\t\t\t\t</Property>\n"
            + "\t\t\t\t\t<Property name=\"proj#0..2\">\n" + "\t\t\t\t\t\t{exprs}\t\t\t\t\t</Property>\n"
            + "\t\t\t\t\t<Property name=\"$condition\">\n" + "\t\t\t\t\t\t$t4\t\t\t\t\t</Property>\n"
            + "\t\t\t\t\t<Inputs>\n" + "\t\t\t\t\t\t<RelNode type=\"LogicalTableScan\">\n"
            + "\t\t\t\t\t\t\t<Property name=\"table\">\n" + "\t\t\t\t\t\t\t\t[a]\t\t\t\t\t\t\t</Property>\n"
            + "\t\t\t\t\t\t\t<Inputs/>\n" + "\t\t\t\t\t\t</RelNode>\n" + "\t\t\t\t\t</Inputs>\n"
            + "\t\t\t\t</RelNode>\n" + "\t\t\t</Inputs>\n" + "\t\t</RelNode>\n"
            + "\t\t<RelNode type=\"LogicalExchange\">\n" + "\t\t\t<Property name=\"distribution\">\n"
            + "\t\t\t\tbroadcast\t\t\t</Property>\n" + "\t\t\t<Inputs>\n"
            + "\t\t\t\t<RelNode type=\"LogicalTableScan\">\n" + "\t\t\t\t\t<Property name=\"table\">\n"
            + "\t\t\t\t\t\t[b]\t\t\t\t\t</Property>\n" + "\t\t\t\t\t<Inputs/>\n" + "\t\t\t\t</RelNode>\n"
            + "\t\t\t</Inputs>\n" + "\t\t</RelNode>\n" + "\t</Inputs>\n" + "</RelNode>\n"
        );
  }

  private void testQueryParsing(String query, String digest)
      throws Exception {
    PlannerContext plannerContext = new PlannerContext();
    SqlNode sqlNode = _queryEnvironment.parse(query, plannerContext);
    _queryEnvironment.validate(sqlNode);
    Assert.assertEquals(sqlNode.toString(), digest);
  }

  private TableCache mockTableCache() {
    TableCache mock = mock(TableCache.class);
    when(mock.getSchema("a")).thenReturn(SCHEMA);
    when(mock.getSchema("b")).thenReturn(SCHEMA);
    return mock;
  }

  private RoutingManager getMockRoutingManager() {
    ServerInstance host1 = new ServerInstance(InstanceConfig.toInstanceConfig("localhost_1"));
    ServerInstance host2 = new ServerInstance(InstanceConfig.toInstanceConfig("localhost_2"));
    RoutingManager mock = mock(RoutingManager.class);
    RoutingTable rtA = mock(RoutingTable.class);
    when(rtA.getServerInstanceToSegmentsMap()).thenReturn(ImmutableMap.of(host1, ImmutableList.of("a1", "a2"),
        host2, ImmutableList.of("a3")));
    RoutingTable rtB = mock(RoutingTable.class);
    when(rtB.getServerInstanceToSegmentsMap()).thenReturn(ImmutableMap.of(host2, ImmutableList.of("b1")
    ));
    when(mock.getRoutingTable("a")).thenReturn(rtA);
    when(mock.getRoutingTable("b")).thenReturn(rtB);
    when(mock.getEnabledServerInstanceMap()).thenReturn(ImmutableMap.of("localhost_1", host1, "localhost_2", host2));
    return mock;
  }
}
