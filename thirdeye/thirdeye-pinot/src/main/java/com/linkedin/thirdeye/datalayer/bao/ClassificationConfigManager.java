package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import java.util.List;

public interface ClassificationConfigManager extends AbstractManager<ClassificationConfigDTO> {
  List<ClassificationConfigDTO> findActiveByFunctionId(long functionId);

  ClassificationConfigDTO findByName(String name);
}
