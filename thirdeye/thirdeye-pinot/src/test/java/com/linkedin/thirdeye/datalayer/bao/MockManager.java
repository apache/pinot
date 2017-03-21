package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.List;
import java.util.Map;
import org.testng.Assert;


public class MockManager<T extends AbstractDTO> implements AbstractManager<T> {
  @Override
  public Long save(T entity) {
    Assert.fail();
    return Long.valueOf(0);
  }

  @Override
  public int update(T entity) {
    Assert.fail();
    return 0;
  }

  @Override
  public T findById(Long id) {
    Assert.fail();
    return null;
  }

  @Override
  public void delete(T entity) {
    Assert.fail();
  }

  @Override
  public void deleteById(Long id) {
    Assert.fail();
  }

  @Override
  public List<T> findAll() {
    Assert.fail();
    return null;
  }

  @Override
  public List<T> findByParams(Map<String, Object> filters) {
    Assert.fail();
    return null;
  }

  @Override
  public int update(T entity, Predicate predicate) {
    Assert.fail();
    return 0;
  }
}
