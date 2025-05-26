package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Optional;


public interface TableRowColAuthResult {
  Optional<List<String>> getRLSFilters();
}
