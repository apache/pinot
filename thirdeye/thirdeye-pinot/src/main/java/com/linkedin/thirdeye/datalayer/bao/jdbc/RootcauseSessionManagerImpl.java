package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.RootcauseSessionManager;
import com.linkedin.thirdeye.datalayer.dto.RootcauseSessionDTO;
import com.linkedin.thirdeye.datalayer.pojo.AbstractBean;
import com.linkedin.thirdeye.datalayer.pojo.RootcauseSessionBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;


@Singleton
public class RootcauseSessionManagerImpl extends AbstractManagerImpl<RootcauseSessionDTO> implements RootcauseSessionManager {
  private static final String FIND_BY_LIKE_TEMPLATE = "WHERE %s";
  private static final String FIND_BY_LIKE_JOINER = " AND ";
  private static final String FIND_BY_LIKE_VALUE = "%%%s%%";

  private static final String FIND_BY_NAME_LIKE_TEMPLATE = "name LIKE :name__%d";
  private static final String FIND_BY_NAME_LIKE_KEY = "name__%d";

  public RootcauseSessionManagerImpl() {
    super(RootcauseSessionDTO.class, RootcauseSessionBean.class);
  }

  @Override
  public List<RootcauseSessionDTO> findByName(String name) {
    return findByPredicate(Predicate.EQ("name", name));
  }

  @Override
  public List<RootcauseSessionDTO> findByNameLike(Set<String> nameFragments) {
    return findByLike(nameFragments, FIND_BY_NAME_LIKE_TEMPLATE, FIND_BY_NAME_LIKE_KEY);
  }

  @Override
  public List<RootcauseSessionDTO> findByOwner(String owner) {
    return findByPredicate(Predicate.EQ("owner", owner));
  }

  @Override
  public List<RootcauseSessionDTO> findByAnomalyRange(long start, long end) {
    return findByPredicate(Predicate.AND(Predicate.GT("anomalyRangeEnd", start), Predicate.LT("anomalyRangeStart", end)));
  }

  @Override
  public List<RootcauseSessionDTO> findByCreatedRange(long start, long end) {
    return findByPredicate(Predicate.AND(Predicate.GE("created", start), Predicate.LT("created", end)));
  }

  @Override
  public List<RootcauseSessionDTO> findByUpdatedRange(long start, long end) {
    return findByPredicate(Predicate.AND(Predicate.GE("updated", start), Predicate.LT("updated", end)));
  }

  @Override
  public List<RootcauseSessionDTO> findByPreviousId(long id) {
    return findByPredicate(Predicate.EQ("previousId", id));
  }

  @Override
  public List<RootcauseSessionDTO> findByAnomalyId(long id) {
    return findByPredicate(Predicate.EQ("anomalyId", id));
  }

  private List<RootcauseSessionDTO> findByLike(Set<String> fragments, String template, String key) {
    return findByLike(fragments, template, key, RootcauseSessionDTO.class, RootcauseSessionBean.class);
  }

  private <B extends AbstractBean, D> List<D> findByLike(Set<String> fragments, String template, String key,
      Class<D> dtoClass, Class<B> beanClass) {
    List<String> conditions = new ArrayList<>();
    Map<String, Object> params = new HashMap<>();

    int i = 0;
    for (String fragment : fragments) {
      conditions.add(String.format(template, i));
      params.put(String.format(key, i), String.format(FIND_BY_LIKE_VALUE, fragment));
      i++;
    }

    String query = String.format(FIND_BY_LIKE_TEMPLATE, StringUtils.join(conditions, FIND_BY_LIKE_JOINER));

    List<B> beans = genericPojoDao.executeParameterizedSQL(query, params, beanClass);

    List<D> dtos = new ArrayList<>();
    for (B bean : beans) {
      dtos.add(MODEL_MAPPER.map(bean, dtoClass));
    }

    return dtos;
  }
}
