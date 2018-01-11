package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import java.util.List;

public interface AlertConfigManager extends AbstractManager<AlertConfigDTO> {
  List<AlertConfigDTO> findByActive(boolean active);
  List<AlertConfigDTO> findWhereNameLike(String name);
  AlertConfigDTO findWhereNameEquals(String name);
  List<AlertConfigDTO> findWhereApplicationLike(String appName);
  List<AlertConfigDTO> findByFunctionId(Long functionId);
}
