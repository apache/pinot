package org.apache.pinot.query.catalog;

import com.clearspring.analytics.util.Preconditions;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.spi.data.Schema;


/**
 * Wrapper for pinot internal info for a table.
 */
public class PinotTable extends AbstractTable {
  private Schema _schema;

  public PinotTable(Schema schema) {
    _schema = schema;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    Preconditions.checkState(relDataTypeFactory instanceof TypeFactory);
    TypeFactory typeFactory = (TypeFactory) relDataTypeFactory;
    return typeFactory.createRelDataTypeFromSchema(_schema);
  }

  @Override
  public boolean isRolledUp(String s) {
    return false;
  }
}
