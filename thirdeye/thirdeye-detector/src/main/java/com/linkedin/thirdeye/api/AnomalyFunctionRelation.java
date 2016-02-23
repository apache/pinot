package com.linkedin.thirdeye.api;

import com.google.common.base.MoreObjects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Entity
@Table(name = "anomaly_function_relations")
@NamedQueries({
    @NamedQuery(name = "com.linkedin.thirdeye.api.AnomalyFunctionRelation#find", query = "SELECT r FROM AnomalyFunctionRelation r"),
    @NamedQuery(name = "com.linkedin.thirdeye.api.AnomalyFunctionRelation#findByParent", query = "SELECT r FROM AnomalyFunctionRelation r WHERE r.parentId = :parentId"),
    @NamedQuery(name = "com.linkedin.thirdeye.api.AnomalyFunctionRelation#deleteByParent", query = "DELETE FROM AnomalyFunctionRelation r WHERE r.parentId = :parentId")
})
public class AnomalyFunctionRelation implements Serializable {
  private static final long serialVersionUID = 7526472295622776147L;

  @Id
  @Column(name = "parent_id", nullable = false)
  private long parentId;

  @Id
  @Column(name = "child_id", nullable = false)
  private long childId;

  public AnomalyFunctionRelation() {
  }

  public long getParentId() {
    return parentId;
  }

  public void setParentId(long parentId) {
    this.parentId = parentId;
  }

  public long getChildId() {
    return childId;
  }

  public void setChildId(long childId) {
    this.childId = childId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("parentId", parentId).add("childId", childId)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AnomalyFunctionRelation)) {
      return false;
    }
    AnomalyFunctionRelation r = (AnomalyFunctionRelation) o;
    return Objects.equals(parentId, r.getParentId()) && Objects.equals(childId, r.getChildId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(parentId, childId);
  }
}
