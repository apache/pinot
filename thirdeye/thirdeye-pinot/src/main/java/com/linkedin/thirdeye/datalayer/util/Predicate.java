package com.linkedin.thirdeye.datalayer.util;

import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.common.collect.Range;

public class Predicate {
  enum OPER {
    AND("AND"), OR("OR"), EQ("="), GT(">"), LT("<"), NEQ("!="), IN("IN"), BETWEEN("BETWEEN");
    private String sign;

    OPER(String sign) {
      this.sign = sign;
    }

    @Override
    public String toString() {
      return sign;
    }

  }

  private String lhs;
  private OPER oper;
  private Object rhs;
  private List<Predicate> childPredicates;

  private Predicate(String lhs, OPER oper, Object rhs) {
    this.lhs = lhs;
    this.oper = oper;
    this.rhs = rhs;
  }

  private Predicate(OPER oper, List<Predicate> childPredicates) {
    this.childPredicates = childPredicates;
    this.oper = oper;
  }

  public String getLhs() {
    return lhs;
  }

  public OPER getOper() {
    return oper;
  }

  public Object getRhs() {
    return rhs;
  }

  public List<Predicate> getChildPredicates() {
    return childPredicates;
  }

  public static Predicate EQ(String columnName, Object value) {
    return new Predicate(columnName, OPER.EQ, value);
  }
  public static Predicate NEQ(String columnName, Object value) {
    return new Predicate(columnName, OPER.NEQ, value);
  }

  public static Predicate LT(String columnName, Object value) {
    return new Predicate(columnName, OPER.LT, value);
  }

  public static Predicate GT(String columnName, Object value) {
    return new Predicate(columnName, OPER.GT, value);
  }

  public static Predicate AND(Predicate... childPredicates) {
    return new Predicate(null, OPER.AND, null);
  }

  public static Predicate OR(Predicate... childPredicates) {
    return new Predicate(null, OPER.OR, null);
  }

  public static Predicate BETWEEN(String columnName, Object startValue, Object endValue) {
    return new Predicate(columnName, OPER.BETWEEN,
        new ImmutablePair<Object, Object>(startValue, endValue));
  }

}
