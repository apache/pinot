package org.apache.pinot.query.catalog;

import java.util.List;
import org.apache.calcite.jdbc.CalciteSchema;


public class CatalogSchema extends CalciteSchema {
  public List<String> getDefaultSchema() {
    return null;
  }
}
