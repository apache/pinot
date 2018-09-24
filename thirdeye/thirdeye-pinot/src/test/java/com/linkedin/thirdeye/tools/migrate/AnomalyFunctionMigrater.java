package com.linkedin.thirdeye.tools.migrate;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.Map;


public interface AnomalyFunctionMigrater {
  /**
   * Migrate the properties of the given functionDTO to the new properties setup
   * @param functionDTO
   */
  void migrate(AnomalyFunctionDTO functionDTO);

  /**
   * Get the defaultProperties
   * @return
   */
  Map<String, String> getDefaultProperties();
}
