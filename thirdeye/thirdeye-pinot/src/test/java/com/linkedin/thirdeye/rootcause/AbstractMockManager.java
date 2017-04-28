package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.datalayer.bao.AbstractManager;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.List;
import java.util.Map;


public abstract class AbstractMockManager<T extends AbstractDTO> implements AbstractManager<T> {
  @Override
  public Long save(T entity) {
    throw new AssertionError("not implemented");
  }

  @Override
  public int update(T entity) {
    throw new AssertionError("not implemented");
  }

  @Override
  public T findById(Long id) {
    throw new AssertionError("not implemented");
  }

  @Override
  public void delete(T entity) {
    throw new AssertionError("not implemented");
  }

  @Override
  public void deleteById(Long id) {
    throw new AssertionError("not implemented");
  }

  @Override
  public List<T> findAll() {
    throw new AssertionError("not implemented");
  }

  @Override
  public List<T> findByParams(Map<String, Object> filters) {
    throw new AssertionError("not implemented");
  }

  @Override
  public List<T> findByPredicate(Predicate predicate) {
    throw new AssertionError("not implemented");
  }

  @Override
  public int update(T entity, Predicate predicate) {
    throw new AssertionError("not implemented");
  }
}
