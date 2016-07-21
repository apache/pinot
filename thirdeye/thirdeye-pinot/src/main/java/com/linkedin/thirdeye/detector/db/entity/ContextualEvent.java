package com.linkedin.thirdeye.detector.db.entity;

import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import com.google.common.base.MoreObjects;

// TODO rename this to just "Event". Also rename resources, DAO, sample files, etc.
@Entity
@Table(name = "contextual_events")
@NamedQueries({
    @NamedQuery(name = "com.linkedin.thirdeye.api.ContextualEvent#findAllByTime", query = "SELECT e FROM ContextualEvent e WHERE e.timeUtc >= :startTimeUtc AND e.timeUtc <= :endTimeUtc")
})
public class ContextualEvent extends AbstractBaseEntity implements Comparable<ContextualEvent> {
  @Column(name = "time_utc", nullable = false)
  private long timeUtc;

  @Column(name = "title", nullable = false)
  private String title;

  @Column(name = "description", nullable = true)
  private String description;

  @Column(name = "reference", nullable = true)
  private String reference;

  public long getTimeUtc() {
    return timeUtc;
  }

  public void setTimeUtc(long timeUtc) {
    this.timeUtc = timeUtc;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getReference() {
    return reference;
  }

  public void setReference(String reference) {
    this.reference = reference;
  }

  @Override
  public int compareTo(ContextualEvent o) {
    return (int) (timeUtc - o.getTimeUtc());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("timeUtc", timeUtc)
        .add("title", title).add("description", description).add("reference", reference).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ContextualEvent)) {
      return false;
    }
    ContextualEvent e = (ContextualEvent) o;
    return Objects.equals(getId(), e.getId()) && Objects.equals(timeUtc, e.getTimeUtc())
        && Objects.equals(title, e.getTitle()) && Objects.equals(description, e.getDescription())
        && Objects.equals(reference, e.getReference());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), timeUtc, title, description, reference);
  }
}
