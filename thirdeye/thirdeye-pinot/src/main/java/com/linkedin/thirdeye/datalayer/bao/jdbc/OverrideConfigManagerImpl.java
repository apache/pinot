package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.OverrideConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.List;

@Singleton
public class OverrideConfigManagerImpl extends AbstractManagerImpl<OverrideConfigDTO> implements
    OverrideConfigManager {

  public OverrideConfigManagerImpl() {
    super(OverrideConfigDTO.class, OverrideConfigBean.class);
  }

  @Override
  public List<OverrideConfigDTO> findAllConflictByTargetType(String entityTypeName,
      long windowStart, long windowEnd) {
    Predicate predicate =
        Predicate.AND(Predicate.LE("startTime", windowEnd), Predicate.GE("endTime", windowStart),
            Predicate.EQ("targetEntity", entityTypeName));

    return findByPredicate(predicate);
  }

  @Override
  public List<OverrideConfigDTO> findAllConflict(long windowStart, long windowEnd) {
    Predicate predicate =
        Predicate.AND(Predicate.LE("startTime", windowEnd), Predicate.GE("endTime", windowStart));

    return findByPredicate(predicate);
  }
}
