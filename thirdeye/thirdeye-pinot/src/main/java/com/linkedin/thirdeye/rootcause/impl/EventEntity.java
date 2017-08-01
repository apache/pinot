package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import java.util.ArrayList;
import java.util.List;


/**
 * EventEntity represents an individual event. It holds meta-data referencing ThirdEye's internal
 * database. The URN namespace is defined as 'thirdeye:event:{type}:{id}'.
 */
public class EventEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:event:");

  private final String eventType;
  private final long id;

  protected EventEntity(String urn, double score, List<? extends Entity> related, String eventType, long id) {
    super(urn, score, related);
    this.id = id;
    this.eventType = eventType;
  }

  public String getEventType() {
    return eventType;
  }

  public long getId() {
    return id;
  }

  @Override
  public EventEntity withScore(double score) {
    return new EventEntity(this.getUrn(), score, this.getRelated(), this.eventType, this.id);
  }

  @Override
  public EventEntity withRelated(List<? extends Entity> related) {
    return new EventEntity(this.getUrn(), this.getScore(), related, this.eventType, this.id);
  }

  public static EventEntity fromURN(String urn, double score) {
    if(!TYPE.isType(urn))
      throw new IllegalArgumentException(String.format("URN '%s' is not type '%s'", urn, TYPE.getPrefix()));
    String[] parts = urn.split(":", 4);
    if(parts.length != 4)
      throw new IllegalArgumentException(String.format("URN must have 3 parts but has '%d'", parts.length));
    return new EventEntity(urn, score, new ArrayList<Entity>(), parts[2], Long.parseLong(parts[3]));
  }
}
