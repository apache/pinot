package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RootCauseEntity {
  String urn;
  double score;
  String label;
  String type;
  String link;
  List<RootCauseEntity> relatedEntities = new ArrayList<>();
  Multimap<String, String> attributes = ArrayListMultimap.create();

  public RootCauseEntity() {
    // left blank
  }

  public RootCauseEntity(String urn, double score, String label, String type, String link) {
    this.urn = urn;
    this.score = score;
    this.label = label;
    this.type = type;
    this.link = link;
  }

  public String getUrn() {
    return urn;
  }

  public void setUrn(String urn) {
    this.urn = urn;
  }

  public double getScore() {
    return score;
  }

  public void setScore(double score) {
    this.score = score;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getLink() {
    return link;
  }

  public void setLink(String link) {
    this.link = link;
  }

  public List<RootCauseEntity> getRelatedEntities() {
    return relatedEntities;
  }

  public void setRelatedEntities(List<RootCauseEntity> relatedEntities) {
    this.relatedEntities = relatedEntities;
  }

  public void addRelatedEntity(RootCauseEntity e) {
    this.relatedEntities.add(e);
  }

  public Multimap<String, String> getAttributes() {
    return attributes;
  }

  public void setAttributes(Multimap<String, String> attributes) {
    this.attributes = attributes;
  }

  public void putAttribute(String key, String value) {
    this.attributes.put(key, value);
  }
}
