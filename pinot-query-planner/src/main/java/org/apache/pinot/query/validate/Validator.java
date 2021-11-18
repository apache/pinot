package org.apache.pinot.query.validate;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;


public class Validator extends SqlValidatorImpl {

  public Validator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory) {
    super(opTab, catalogReader, typeFactory, SqlConformanceEnum.DEFAULT);
  }
}
