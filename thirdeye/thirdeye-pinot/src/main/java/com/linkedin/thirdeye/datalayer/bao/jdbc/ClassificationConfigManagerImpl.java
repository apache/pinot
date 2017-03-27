package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.ClassificationConfigManager;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.ClassificationConfigBean;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;

public class ClassificationConfigManagerImpl extends AbstractManagerImpl<ClassificationConfigDTO>
    implements ClassificationConfigManager {

  protected ClassificationConfigManagerImpl() {
    super(ClassificationConfigDTO.class, ClassificationConfigBean.class);
  }

  @Override
  public List<ClassificationConfigDTO> findActiveByFunctionId(long functionId) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("mainFunctionId", functionId);
    filters.put("active", true);
    List<ClassificationConfigDTO> configs = super.findByParams(filters);
    if (CollectionUtils.isNotEmpty(configs)) {
      return configs;
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public ClassificationConfigDTO findByName(String name) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("name", name);
    List<ClassificationConfigDTO> configs = super.findByParams(filters);
    if (CollectionUtils.isNotEmpty(configs)) {
      return configs.get(0);
    } else {
      return null;
    }
  }
}
