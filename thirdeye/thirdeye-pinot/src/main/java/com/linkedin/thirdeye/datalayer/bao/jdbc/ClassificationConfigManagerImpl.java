package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.ClassificationConfigManager;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.ClassificationConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;

public class ClassificationConfigManagerImpl extends AbstractManagerImpl<ClassificationConfigDTO>
    implements ClassificationConfigManager {

  protected ClassificationConfigManagerImpl() {
    super(ClassificationConfigDTO.class, ClassificationConfigBean.class);
  }

  @Override
  public List<ClassificationConfigDTO> findActiveByFunctionId(long functionId) {
    Predicate predicate = Predicate.AND(
        Predicate.EQ("mainFunctionId", functionId),
        Predicate.EQ("active", true));
    List<ClassificationConfigBean> configBeenList = genericPojoDao.get(predicate, ClassificationConfigBean.class);
    List<ClassificationConfigDTO> results = new ArrayList<>();
    for (ClassificationConfigBean bean : configBeenList) {
      results.add(MODEL_MAPPER.map(bean, ClassificationConfigDTO.class));
    }
    return results;
  }

  @Override
  public ClassificationConfigDTO findByName(String name) {
    Predicate predicate = Predicate.EQ("name", name);

    List<ClassificationConfigBean> configBeenList = genericPojoDao.get(predicate, ClassificationConfigBean.class);
    List<ClassificationConfigDTO> results = new ArrayList<>();
    for (ClassificationConfigBean bean : configBeenList) {
      results.add(MODEL_MAPPER.map(bean, ClassificationConfigDTO.class));
    }
    if (CollectionUtils.isNotEmpty(results)) {
      return results.get(0);
    } else {
      return null;
    }
  }
}
