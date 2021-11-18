package org.apache.pinot.query.type;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;


/**
 * Extends Java-base TypeFactory
 */
public class TypeFactory extends JavaTypeFactoryImpl {
  private RelDataTypeSystem _typeSystem;

  public TypeFactory(RelDataTypeSystem typeSystem) {
    _typeSystem = typeSystem;
  }
}
