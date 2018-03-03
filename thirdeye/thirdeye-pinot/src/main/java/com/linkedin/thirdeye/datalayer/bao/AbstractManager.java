package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.util.Predicate;


public interface AbstractManager<E extends AbstractDTO> {

  Long save(E entity);

  int update(E entity);

  E findById(Long id);

  int delete(E entity);

  int deleteById(Long id);

  int deleteByIds(List<Long> ids);

  int deleteByPredicate(Predicate predicate);

  List<E> findAll();

  List<E> findByParams(Map<String, Object> filters);

  List<E> findByPredicate(Predicate predicate);

  List<Long> findIdsByPredicate(Predicate predicate);

  int update(E entity, Predicate predicate);
}
