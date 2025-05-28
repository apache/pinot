package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Map;
import java.util.Optional;


public interface MultiTableRowColAuthResult {
  Optional<Map<String, List<String>>> getRLSFilterForTable(String tableName);
}
