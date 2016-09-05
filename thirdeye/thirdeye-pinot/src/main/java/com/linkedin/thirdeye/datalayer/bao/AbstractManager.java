package com.linkedin.thirdeye.datalayer.bao;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

public class AbstractManager<E extends AbstractDTO> {

  @Inject
  private Provider<EntityManager> emf;

  final Class<E> entityClass;

  AbstractManager(Class<E> entityClass) {
    this.entityClass = entityClass;
  }

  EntityManager getEntityManager() {
    return emf.get();
  }

  @Transactional(rollbackOn = Exception.class)
  public Long save(E entity) {
    getEntityManager().persist(entity);
    return entity.getId();
  }

  @Transactional(rollbackOn = Exception.class)
  public void update(E entity) {
    getEntityManager().merge(entity);
  }

  public void updateAll(List<E> entities) {
    entities.forEach(this::update);
  }

  @Transactional
  public E findById(Long id) {
    return getEntityManager().find(entityClass, id);
  }

  @Transactional
  public void delete(E entity) {
    getEntityManager().remove(entity);
  }

  public void deleteById(Long id) {
    getEntityManager().remove(findById(id));
  }

  @Transactional
  public List<E> findAll() {
    return getEntityManager().createQuery("from " + entityClass.getSimpleName(), entityClass)
        .getResultList();
  }

  @Transactional
  public List<E> findByParams(Map<String, Object> filters) {
    CriteriaBuilder builder = getEntityManager().getCriteriaBuilder();
    CriteriaQuery<E> query = builder.createQuery(entityClass);
    Root<E> entityRoot = query.from(entityClass);
    List<Predicate> predicates = new ArrayList<>();
    for (Map.Entry<String, Object> entry : filters.entrySet()) {
      if  (entry.getValue() == null ) {
        predicates.add(builder.isNull(entityRoot.get(entry.getKey())));
      } else {
        predicates.add(builder.equal(entityRoot.get(entry.getKey()), entry.getValue()));
      }
    }
    if (predicates.size() > 0) {
      query.where(predicates.toArray(new Predicate[0]));
    }
    return getEntityManager().createQuery(query).getResultList();
  }
}
