package org.apache.pinot.query.type;

import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * Extends Java-base TypeFactory
 */
public class TypeFactory extends JavaTypeFactoryImpl {
  private final RelDataTypeSystem _typeSystem;

  public TypeFactory(RelDataTypeSystem typeSystem) {
    _typeSystem = typeSystem;
  }

  public RelDataType createRelDataTypeFromSchema(Schema schema) {
    Builder builder = new Builder(this);
    for (Map.Entry<String, FieldSpec> e : schema.getFieldSpecMap().entrySet()) {
      builder.add(e.getKey(), toRelDataType(e.getValue()));
    }
    return builder.build();
  }

  private RelDataType toRelDataType(FieldSpec fieldSpec) {
    switch (fieldSpec.getDataType()) {
      case INT:
        return this.createSqlType(SqlTypeName.INTEGER);
      case LONG:
        return this.createSqlType(SqlTypeName.BIGINT);
      case FLOAT:
        return this.createSqlType(SqlTypeName.FLOAT);
      case DOUBLE:
        return this.createSqlType(SqlTypeName.DOUBLE);
      case BOOLEAN:
        return this.createSqlType(SqlTypeName.BOOLEAN);
      case TIMESTAMP:
        return this.createSqlType(SqlTypeName.TIMESTAMP);
      case STRING:
        return this.createSqlType(SqlTypeName.VARCHAR);
      case BYTES:
        return this.createSqlType(SqlTypeName.VARBINARY);
      case JSON:
      case STRUCT:
      case MAP:
      case LIST:
      default:
        throw new UnsupportedOperationException("unsupported!");
    }
  }
}
