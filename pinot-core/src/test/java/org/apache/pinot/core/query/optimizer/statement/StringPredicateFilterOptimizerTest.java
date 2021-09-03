package org.apache.pinot.core.query.optimizer.statement;

import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.Test;


public class StringPredicateFilterOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final CalciteSqlCompiler SQL_COMPILER = new CalciteSqlCompiler();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("intColumn1", FieldSpec.DataType.INT)
          .addSingleValueDimension("intColumn2", FieldSpec.DataType.INT)
          .addSingleValueDimension("strColumn1", FieldSpec.DataType.STRING)
          .addSingleValueDimension("strColumn2", FieldSpec.DataType.STRING).build();
  private static final TableConfig TABLE_CONFIG_WITHOUT_INDEX = null;

  @Test
  public void testReplaceMinusWithCompare() {
    // 'WHERE strColumn1=strColumn2' gets replaced with 'compare(strColumn1, strColumn2) = 0'
    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE strColumn1=strColumn2",
        "SELECT * FROM testTable WHERE compare(strColumn1,strColumn2) = 0", TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);

    // 'WHERE strColumn1>strColumn2' gets replaced with 'compare(strColumn1, strColumn2) > 0'
    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE strColumn1>strColumn2",
        "SELECT * FROM testTable WHERE compare(strColumn1,strColumn2) > 0", TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);

    // 'HAVING strColumn1=strColumn2' gets replaced with 'compare(strColumn1, strColumn2) = 0'
    TestHelper.assertEqualsQuery("SELECT strColumn1, strColumn2 FROM testTable HAVING strColumn1=strColumn2",
        "SELECT strColumn1, strColumn2 FROM testTable HAVING compare(strColumn1,strColumn2)=0",
        TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);

    // 'HAVING strColumn1=strColumn2' gets replaced with 'compare(strColumn1, strColumn2) < 0'
    TestHelper.assertEqualsQuery("SELECT strColumn1, strColumn2 FROM testTable HAVING strColumn1<strColumn2",
        "SELECT strColumn1, strColumn2 FROM testTable HAVING compare(strColumn1,strColumn2)<0",
        TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);
  }
}
