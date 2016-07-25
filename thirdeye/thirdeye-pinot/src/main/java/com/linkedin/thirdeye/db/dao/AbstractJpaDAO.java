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

  protected final Class<E> entityClass;

  public AbstractJpaDAO(Class<E> entityClass) {
    this.entityClass = entityClass;
  }

  public EntityManager getEntityManager() {
    return emf.get();
  }

  @Transactional
  public Long save(E entity) {
    emf.get().persist(entity);
    return entity.getId();
  }

  @Transactional
  public void update(E entity) {
    emf.get().merge(entity);
  }

  public E findById(Long id) {
    return emf.get().find(entityClass, id);
  }

  @Transactional
  public void delete(E entity) {
    emf.get().remove(entity);
  }

  @Transactional
  public void deleteById(Long id) {
    emf.get().remove(findById(id));
  }

  public List<E> findAll() {
    return this.emf.get().createNamedQuery(
        entityClass.getSimpleName() + ".GetAll").getResultList();
  }
}
