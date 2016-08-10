package com.linkedin.thirdeye.db.dao;

import com.google.inject.Inject;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.db.entity.AbstractBaseEntity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

public class AbstractJpaDAO<E extends AbstractBaseEntity> {

  @Inject
  private EntityManager em;

  final Class<E> entityClass;
  AbstractJpaDAO(Class<E> entityClass) {
    this.entityClass = entityClass;
  }
  EntityManager getEntityManager() {
    return em;
  }

  @Transactional
  public Long save(E entity) {
    getEntityManager().persist(entity);
    return entity.getId();
  }

  @Transactional
  public void update(E entity) {
    getEntityManager().merge(entity);
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

  public List<E> findByParams(Map<String, Object> filters) {
    CriteriaBuilder builder = getEntityManager().getCriteriaBuilder();
    CriteriaQuery<E> query = builder.createQuery(entityClass);
    Root<E> entityRoot = query.from(entityClass);
    List<Predicate> predicates = new ArrayList<>();
    for (Map.Entry<String, Object> entry : filters.entrySet()) {
      predicates.add(builder.equal(entityRoot.get(entry.getKey()), entry.getValue()));
    }
    if (predicates.size() > 0) {
      query.where(predicates.toArray(new Predicate[0]));
    }
    return getEntityManager().createQuery(query).getResultList();
  }
}
