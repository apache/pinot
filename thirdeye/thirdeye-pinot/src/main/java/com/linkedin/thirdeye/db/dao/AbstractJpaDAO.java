package com.linkedin.thirdeye.db.dao;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.db.entity.AbstractBaseEntity;
import java.util.List;
import javax.persistence.EntityManager;

public class AbstractJpaDAO<E extends AbstractBaseEntity> {
  @Inject
  private Provider<EntityManager> emf;

  final Class<E> entityClass;
  AbstractJpaDAO(Class<E> entityClass) {
    this.entityClass = entityClass;
  }
  EntityManager getEntityManager() {
    return emf.get();
  }

  @Transactional
  public Long save(E entity) {
    getEntityManager().persist(entity);
    return entity.getId();
  }

  @Transactional
  public void update(E entity) {
    emf.get().merge(entity);
  }

  public E findById(Long id) {
    return getEntityManager().find(entityClass, id);
  }

  @Transactional
  public void delete(E entity) {
    getEntityManager().remove(entity);
  }

  @Transactional
  public void deleteById(Long id) {
    getEntityManager().remove(findById(id));
  }

  public List<E> findAll() {
    return getEntityManager().createQuery("from " + entityClass.getSimpleName(), entityClass)
        .getResultList();
  }
}
