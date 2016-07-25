package com.linkedin.thirdeye.detector.db.dao;

import com.linkedin.thirdeye.db.entity.AbstractBaseEntity;
import io.dropwizard.hibernate.AbstractDAO;
import io.dropwizard.hibernate.UnitOfWork;
import org.hibernate.SessionFactory;

@Deprecated
// Use @link com.linkedin.thirdeye.db.dao.AbstractJpaDAO
public abstract class AbstractBaseDAO<E extends AbstractBaseEntity> extends AbstractDAO<E> {

  public AbstractBaseDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  /**
   * Persist the indicated entity to database - create or update
   *
   * @param entity
   *
   * @return the primary key
   */
  @UnitOfWork
  public Long save(E entity) {
    return persist(entity).getId();
  }

  /**
   * Retrieve an object using indicated ID
   *
   * @param id
   *
   * @return
   */
  @UnitOfWork
  public E findById(Long id) {
    return get(id);
  }

  /**
   * Delete by id
   *
   * @param id
   */
  @UnitOfWork
  public void deleteById(Long id) {
    E entity = findById(id);
    if (entity != null) {
      delete(entity);
    }
  }

  /**
   * Delete indicated entity from database
   *
   * @param entity
   */
  @UnitOfWork
  public void delete(E entity) {
    currentSession().delete(entity);
  }
}
