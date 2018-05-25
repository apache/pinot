package com.linkedin.thirdeye.datalayer.entity;

import com.linkedin.thirdeye.datalayer.pojo.SessionBean;


public class SessionIndex extends AbstractIndexEntity {
  String sessionKey;
  SessionBean.PrincipalType principalType;

  public String getSessionKey() {
    return sessionKey;
  }

  public void setSessionKey(String sessionKey) {
    this.sessionKey = sessionKey;
  }

  public SessionBean.PrincipalType getPrincipalType() {
    return principalType;
  }

  public void setPrincipalType(SessionBean.PrincipalType principalType) {
    this.principalType = principalType;
  }
}
