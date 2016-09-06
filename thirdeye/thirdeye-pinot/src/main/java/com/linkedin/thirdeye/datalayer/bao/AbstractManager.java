package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;


public interface AbstractManager<E extends AbstractDTO> {

  Long save(E entity);

  void update(E entity);

  void updateAll(List<E> entities);

  E findById(Long id);

  void delete(E entity);

  void deleteById(Long id);

  List<E> findAll();

  List<E> findByParams(Map<String, Object> filters);
}
