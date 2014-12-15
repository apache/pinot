package com.linkedin.pinot.core.common;

import java.util.Arrays;
import java.util.List;


public class Predicate {

  public enum Type {
    EQ,
    NEQ,
    REGEX,
    RANGE,
    IN,
    NOT_IN
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

  @Override
  public String toString() {
    return "Predicate: type: " + type + ", left : " + lhs + ", right : " + Arrays.toString(rhs.toArray(new String[0]))
        + "\n";
  }

}
