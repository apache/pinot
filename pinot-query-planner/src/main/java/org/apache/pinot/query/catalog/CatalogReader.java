package org.apache.pinot.query.catalog;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;


public class CatalogReader extends CalciteCatalogReader {

  public CatalogReader(CatalogSchema catalogSchema, RelDataTypeFactory typeFactory,
      CalciteConnectionConfig config) {
    super(catalogSchema, catalogSchema.getDefaultSchema(), typeFactory, config);
  }
}
