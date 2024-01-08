package org.apache.pinot.core.query.aggregation.function;

import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class LastWithTimeAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @DataProvider(name = "scenarios")
  Object[] scenarios() {
    return new Object[] {
        new Scenario(FieldSpec.DataType.INT, "1", "2", "-2147483648"),
        new Scenario(FieldSpec.DataType.LONG, "1", "2", "-9223372036854775808"),
        new Scenario(FieldSpec.DataType.FLOAT, "1", "2", "-Infinity"),
        new Scenario(FieldSpec.DataType.DOUBLE, "1", "2", "-Infinity"),
        new Scenario(FieldSpec.DataType.STRING, "a", "b", "\"null\""),
    };
  }

  public class Scenario {
    private final PinotDataType _pinotDataType;
    private final FieldSpec.DataType _dataType;
    private final String _valAsStr1;
    private final String _valAsStr2;
    private final String _defaultNullValue;

    public Scenario(FieldSpec.DataType dataType, String valAsStr1, String valAsStr2, String defaultNullValue) {
      _dataType = dataType;
      _valAsStr1 = valAsStr1;
      _valAsStr2 = valAsStr2;
      _defaultNullValue = defaultNullValue;
      _pinotDataType =
          _dataType == FieldSpec.DataType.INT ? PinotDataType.INTEGER : PinotDataType.valueOf(_dataType.name());
    }

    public FluentQueryTest.DeclaringTable getDeclaringTable(boolean nullHandlingEnabled) {
      Schema schema = new Schema.SchemaBuilder()
          .setSchemaName("testTable")
          .setEnableColumnBasedNullHandling(true)
          .addDimensionField("myField", _dataType, f -> f.setNullable(true))
          .addDimensionField("timeField", FieldSpec.DataType.TIMESTAMP)
          .build();
      TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.OFFLINE)
          .setTableName("testTable");

      return FluentQueryTest.withBaseDir(_baseDir)
          .withNullHandling(nullHandlingEnabled)
          .givenTable(schema, tableConfigBuilder.build());
    }

    @Override
    public String toString() {
      return "Scenario{" + "dt=" + _dataType + ", val1='" + _valAsStr1 + '\'' + ", val2='"
          + _valAsStr2 + '\'' + '}';
    }
  }

  @Test(dataProvider = "scenarios")
  void aggrWithoutNull(Scenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField | timeField",
            "null                   | 1",
            scenario._valAsStr1 + " | 2",
            "null                   | 3"
        ).andOnSecondInstance("myField | timeField",
            "null                   | 4",
            scenario._valAsStr2 + " | 5",
            "null                   | 6"
        )
        .whenQuery("select LAST_WITH_TIME(myField, timeField, '" + scenario._dataType + "') from testTable")
        .thenResultIs(scenario._pinotDataType.name(), scenario._defaultNullValue);
  }

  @Test(dataProvider = "scenarios")
  void aggrWithNull(Scenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField | timeField",
            "null                   | 1",
            scenario._valAsStr1 + " | 2",
            "null                   | 3"
        ).andOnSecondInstance("myField | timeField",
            "null                   | 4",
            scenario._valAsStr2 + " | 5",
            "null                   | 6"
        )
        .whenQuery("select LAST_WITH_TIME(myField, timeField, '" + scenario._dataType + "') from testTable")
        .thenResultIs(scenario._pinotDataType.name(), scenario._valAsStr2);
  }

  @Test(dataProvider = "scenarios")
  void aggrSvWithoutNull(Scenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField | timeField",
            "null                   | 1",
            scenario._valAsStr1 + " | 2",
            "null                   | 3"
        ).andOnSecondInstance("myField | timeField",
            "null                   | 4",
            scenario._valAsStr2 + " | 5",
            "null                   | 6"
        ).whenQuery("select 'cte', LAST_WITH_TIME(myField, timeField, '" + scenario._dataType + "') as mode "
            + "from testTable "
            + "group by 'cte'")
        .thenResultIs("STRING | " + scenario._pinotDataType.name(), "cte | " + scenario._defaultNullValue);
  }

  @Test(dataProvider = "scenarios")
  void aggrSvWithNull(Scenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField | timeField",
            "null                   | 1",
            scenario._valAsStr1 + " | 2",
            "null                   | 3"
        ).andOnSecondInstance("myField | timeField",
            "null                   | 4",
            scenario._valAsStr2 + " | 5",
            "null                   | 6"
        ).whenQuery("select 'cte', LAST_WITH_TIME(myField, timeField, '" + scenario._dataType + "') as mode "
            + "from testTable "
            + "group by 'cte'")
        .thenResultIs("STRING | " + scenario._pinotDataType.name(), "cte | " + scenario._valAsStr2);
  }

  @Test(dataProvider = "scenarios")
  void aggrMvWithoutNull(Scenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField | timeField",
            "null                   | 1",
            scenario._valAsStr1 + " | 2",
            "null                   | 3"
        ).andOnSecondInstance("myField | timeField",
            "null                   | 4",
            scenario._valAsStr2 + " | 5",
            "null                   | 6"
        ).whenQuery("select 'cte1' as cte1, 'cte2' as cte2, "
            + "LAST_WITH_TIME(myField, timeField, '" + scenario._dataType + "') as mode "
            + "from testTable "
            + "group by 'cte'")
        .thenResultIs("STRING | STRING | " + scenario._pinotDataType.name(),
            "cte1 | cte2 | " + scenario._defaultNullValue);
  }

  @Test(dataProvider = "scenarios")
  void aggrMvWithNull(Scenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField | timeField",
            "null                   | 1",
            scenario._valAsStr1 + " | 2",
            "null                   | 3"
        ).andOnSecondInstance("myField | timeField",
            "null                   | 4",
            scenario._valAsStr2 + " | 5",
            "null                   | 6"
        ).whenQuery("select 'cte1' as cte1, 'cte2' as cte2, "
            + "LAST_WITH_TIME(myField, timeField, '" + scenario._dataType + "') as mode "
            + "from testTable "
            + "group by 'cte'")
        .thenResultIs("STRING | STRING | " + scenario._pinotDataType.name(),
            "cte1 | cte2 | " + scenario._valAsStr2);
  }
}
