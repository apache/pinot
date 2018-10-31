package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.ConfigDTO;
import java.util.List;


public interface ConfigManager extends AbstractManager<ConfigDTO> {
  List<ConfigDTO> findByNamespace(String namespace);
  ConfigDTO findByNamespaceName(String namespace, String name);
  void deleteByNamespaceName(String namespace, String name);
}
