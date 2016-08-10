package com.linkedin.thirdeye.db.dao;

import java.util.HashMap;
import java.util.List;

import com.linkedin.thirdeye.db.entity.AnomalyFunctionRelation;
import java.util.Map;

public class AnomalyFunctionRelationDAO extends AbstractJpaDAO<AnomalyFunctionRelation> {
  public AnomalyFunctionRelationDAO() {
    super(AnomalyFunctionRelation.class);
  }

  private static final String DELETE_BY_PARENT_ID = "DELETE FROM AnomalyFunctionRelation r WHERE r.parentId = :parentId";
  private static final String DELETE_BY_PARENT_CHILD = "DELETE FROM AnomalyFunctionRelation r WHERE r.parentId = :parentId and r.childId = :childId";

  public void deleteByParent(Long parentId) {
    getEntityManager().createQuery(DELETE_BY_PARENT_ID, entityClass)
        .setParameter("parentId", parentId).executeUpdate();
  }

  public void deleteByParentChild(Long parentId, Long childId) {
    getEntityManager().createQuery(DELETE_BY_PARENT_CHILD, entityClass)
        .setParameter("parentId", parentId).setParameter("childId", childId).executeUpdate();
  }

  public List<AnomalyFunctionRelation> findByParent(Long parentId) {
    Map<String, Object> params = new HashMap<>();
    params.put("parentId", parentId);
    return super.findByParams(params);
  }
}
