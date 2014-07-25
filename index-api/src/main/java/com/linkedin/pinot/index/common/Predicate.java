package com.linkedin.pinot.index.common;

import java.util.List;


public class Predicate {

  public enum Type {
    EQ,
    NEQ,
    GT,
    GT_EQ,
    LT,
    LT_EQ,
    REGEX,
    RANGE
  };

  //	public Predicate(String lhs, Type predicateType, int rhs){
  //		this.lhs = lhs;
  //		type = predicateType;
  //		this.rhs = rhs;
  //	}

  public Predicate(String lhs, Type predicateType, List<String> rhs) {
    this.lhs = lhs;
    type = predicateType;
    this.rhs = rhs;
  }
  String lhs;

  List<String> rhs;

  Type type;

  public String getLhs() {
    return lhs;
  }

  public List<String> getRhs() {
    return rhs;
  }

  public Type getType() {
    return type;
  }

}
