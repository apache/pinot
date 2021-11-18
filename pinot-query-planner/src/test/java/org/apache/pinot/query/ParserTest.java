package org.apache.pinot.query;

import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ParserTest {
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
    _queryEnvironment = new QueryEnvironment(new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(mockTableCache())));
  }

  @Test
  public void testSqlStrings() throws Exception {
    testQueryParsing("SELECT * FROM a JOIN b ON a.c1 = b.c2 WHERE a.c3 >= 0",
        "SELECT *\n" + "FROM `a`\n" + "INNER JOIN `b` ON `a`.`c` = `b`.`d`\n" + "WHERE `a`.`e` >= 0");
  }

  private void testQueryParsing(String query, String digest)
      throws Exception {
    QueryContext plannerContext = new PlannerContext();
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
}
