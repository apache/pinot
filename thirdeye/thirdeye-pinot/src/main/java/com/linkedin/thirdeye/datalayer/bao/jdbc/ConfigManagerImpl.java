package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.ConfigManager;
import com.linkedin.thirdeye.datalayer.dto.ConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.ConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.List;


@Singleton
public class ConfigManagerImpl extends AbstractManagerImpl<ConfigDTO> implements ConfigManager {
  public ConfigManagerImpl() {
    super(ConfigDTO.class, ConfigBean.class);
  }

  @Override
  public List<ConfigDTO> findByNamespace(String namespace) {
    return super.findByPredicate(Predicate.EQ("namespace", namespace));
  }

  @Override
  public ConfigDTO findByNamespaceName(String namespace, String name) {
    List<ConfigDTO> configs = super.findByPredicate(Predicate.AND(
        Predicate.EQ("namespace", namespace),
        Predicate.EQ("name", name)
    ));

    if (configs.isEmpty()) {
      return null;
    }

    return configs.get(0);
  }

  @Override
  public void deleteByNamespaceName(String namespace, String name) {
    try {
      super.delete(this.findByNamespaceName(namespace, name));
    } catch (Exception ignore) {
      // left blank
    }
  }
}
