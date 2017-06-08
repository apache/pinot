package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.ClassificationConfigManager;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.ClassificationConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;

@Singleton
public class ClassificationConfigManagerImpl extends AbstractManagerImpl<ClassificationConfigDTO>
    implements ClassificationConfigManager {

  protected ClassificationConfigManagerImpl() {
    super(ClassificationConfigDTO.class, ClassificationConfigBean.class);
  }

  @Override
  public List<ClassificationConfigDTO> findActives() {
    Predicate predicate = Predicate.EQ("active", true);
    return findByPredicate(predicate);
  }

  @Override
  public ClassificationConfigDTO findByName(String name) {
    Predicate predicate = Predicate.EQ("name", name);
    List<ClassificationConfigDTO> results = findByPredicate(predicate);

    if (CollectionUtils.isNotEmpty(results)) {
      return results.get(0);
    } else {
      return null;
    }
  }
}
