package org.apache.pinot.query;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class QueryEnvironmentTest {
  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp() {
    // the port doesn't matter as we are not actually making a server call.
    RoutingManager routingManager = QueryEnvironmentTestUtils.getMockRoutingManager(1, 2);
    _queryEnvironment = new QueryEnvironment(
        new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(QueryEnvironmentTestUtils.mockTableCache())),
        new WorkerManager("localhost", 3, routingManager)
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
    Assert.assertEquals(queryPlan.getQueryStageMap().size(), 4);
    Assert.assertEquals(queryPlan.getStageMetadataMap().size(), 4);
    for (Map.Entry<String, StageMetadata> e : queryPlan.getStageMetadataMap().entrySet()) {
      List<String> tables = e.getValue().getScannedTables();
      if (tables.size() == 1) {
        // table scan stages; for tableA it should have 2 hosts, for tableB it should have only 1
        Assert.assertEquals(
            e.getValue().getServerInstances().stream().map(ServerInstance::toString).collect(Collectors.toList()),
            tables.get(0).equals("a") ? List.of("Server_localhost_1", "Server_localhost_2")
                : List.of("Server_localhost_1"));
      } else if (!e.getKey().equals("ROOT")) {
        // join stage should have both servers used.
        Assert.assertEquals(
            e.getValue().getServerInstances().stream().map(ServerInstance::toString).collect(Collectors.toList()),
            List.of("Server_localhost_1", "Server_localhost_2"));
      } else {
        // reduce stage should have the reducer instance.
        Assert.assertEquals(
            e.getValue().getServerInstances().stream().map(ServerInstance::toString).collect(Collectors.toList()),
            List.of("Server_localhost_3"));
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

    // Assert that relational plan can be written into a ALL-ATTRIBUTE digest.
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    RelWriter planWriter = new RelXmlWriter(pw, SqlExplainLevel.ALL_ATTRIBUTES);
    optimized.explain(planWriter);
    Assert.assertNotNull(sw.toString());
  }

  private void testQueryParsing(String query, String digest)
      throws Exception {
    PlannerContext plannerContext = new PlannerContext();
    SqlNode sqlNode = _queryEnvironment.parse(query, plannerContext);
    _queryEnvironment.validate(sqlNode);
    Assert.assertEquals(sqlNode.toString(), digest);
  }
}
