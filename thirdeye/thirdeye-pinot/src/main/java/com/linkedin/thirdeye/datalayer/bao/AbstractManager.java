package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.util.Predicate;


public interface AbstractManager<E extends AbstractDTO> {

  Long save(E entity);

  int update(E entity);

  E findById(Long id);

  void delete(E entity);

  void deleteById(Long id);

  List<E> findAll();

  List<E> findByParams(Map<String, Object> filters);

  List<E> findByPredicate(Predicate predicate);

  int update(E entity, Predicate predicate);
}
