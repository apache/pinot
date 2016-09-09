package com.linkedin.thirdeye.datalayer.bao.hibernate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.bao.AbstractManager;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;

public class AbstractManagerImpl<E extends AbstractDTO> implements AbstractManager<E> {

  @Inject
  private Provider<EntityManager> emf;

  final Class<E> entityClass;

  AbstractManagerImpl(Class<E> entityClass) {
    this.entityClass = entityClass;
  }

  EntityManager getEntityManager() {
    return emf.get();
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IAbstractManager#save(E)
   */
  @Override
  @Transactional(rollbackOn = Exception.class)
  public Long save(E entity) {
    getEntityManager().persist(entity);
    return entity.getId();
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IAbstractManager#update(E)
   */
  @Override
  @Transactional(rollbackOn = Exception.class)
  public void update(E entity) {
    getEntityManager().merge(entity);
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IAbstractManager#findById(java.lang.Long)
   */
  @Override
  @Transactional
  public E findById(Long id) {
    return getEntityManager().find(entityClass, id);
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IAbstractManager#delete(E)
   */
  @Override
  @Transactional
  public void delete(E entity) {
    deleteById(entity.getId());
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IAbstractManager#deleteById(java.lang.Long)
   */
  @Override
  @Transactional
  public void deleteById(Long id) {
    E entity = getEntityManager().getReference(entityClass, id);
    delete(entity);
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IAbstractManager#findAll()
   */
  @Override
  public List<E> findAll() {
    return getEntityManager().createQuery("from " + entityClass.getSimpleName(), entityClass)
        .getResultList();
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IAbstractManager#findByParams(java.util.Map)
   */
  @Override
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
