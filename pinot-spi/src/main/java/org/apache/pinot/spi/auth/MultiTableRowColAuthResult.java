package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Optional;


public interface MultiTableRowColAuthResult {
  Optional<List<String>> getRLSFilterForTable(String tableName);
}
