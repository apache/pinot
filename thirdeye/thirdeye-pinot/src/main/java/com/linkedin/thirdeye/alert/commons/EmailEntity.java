package com.linkedin.thirdeye.alert.commons;

import org.apache.commons.mail.HtmlEmail;


public class EmailEntity {
  private String from;
  private String to;
  private String subject;
  private HtmlEmail content;

  public EmailEntity() {

  }

  public EmailEntity(String from, String to, String subject, HtmlEmail content) {
    this.from = from;
    this.to = to;
    this.subject = subject;
    this.content = content;
  }

  public String getFrom() {
    return from;
  }

  public void setFrom(String from) {
    this.from = from;
  }

  public String getTo() {
    return to;
  }

  public void setTo(String to) {
    this.to = to;
  }

  public String getSubject() {
    return subject;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

  public HtmlEmail getContent() {
    return content;
  }

  public void setContent(HtmlEmail content) {
    this.content = content;
  }
}
