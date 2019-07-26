package org.apache.pinot.core.operator;

import org.apache.pinot.common.utils.EqualityUtils;


public class OrderByDefn {

  private OrderType _orderType;
  private int _index;
  private boolean _ascending;

  public OrderByDefn(OrderType orderType, int index, boolean ascending) {
    _orderType = orderType;
    _index = index;
    _ascending = ascending;
  }

  public OrderType getOrderType() {
    return _orderType;
  }

  public void setOrderType(OrderType orderType) {
    _orderType = orderType;
  }

  public int getIndex() {
    return _index;
  }

  public void setIndex(int index) {
    _index = index;
  }

  public boolean isAscending() {
    return _ascending;
  }

  public void setAscending(boolean ascending) {
    _ascending = ascending;
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    OrderByDefn that = (OrderByDefn) o;

    return EqualityUtils.isEqual(_index, that._index) && EqualityUtils.isEqual(_orderType, that._orderType)
        && EqualityUtils.isEqual(_ascending, that._ascending);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_orderType);
    result = EqualityUtils.hashCodeOf(result, _index);
    result = EqualityUtils.hashCodeOf(result, _ascending);
    return result;
  }
}
