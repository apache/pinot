package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import java.util.List;

public interface ClassificationConfigManager extends AbstractManager<ClassificationConfigDTO> {
  List<ClassificationConfigDTO> findActives();

  ClassificationConfigDTO findByName(String name);
}
