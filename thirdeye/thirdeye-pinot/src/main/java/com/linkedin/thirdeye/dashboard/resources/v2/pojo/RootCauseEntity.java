package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

public class RootCauseEntity {
  String urn;
  double score;
  String label;
  String type;
  String link;

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
}
