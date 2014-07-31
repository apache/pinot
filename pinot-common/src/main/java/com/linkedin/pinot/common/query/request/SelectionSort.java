package com.linkedin.pinot.common.query.request;

/**
 * SelectionSort specifies how the search results should be sorted. The results can be sorted based on one or multiple
 * fields, in either ascending or descending order. The value of this parameter consists of a list of comma separated
 * strings, each of which can be one of the following values:
 * [column direction] this means that the results should be sorted by [column]
 * in the direction can be either asc or desc.
 * Example : Sort Fields Parameters
 * sort=price:desc,color=asc
 * 
 * @author xiafu
 *
 */
public class SelectionSort {
  public String _column = null;
  public boolean _isAsc = true;

  public SelectionSort(String column, boolean isAsc) {
    _column = column;
    _isAsc = isAsc;
  }

  public String getColumn() {
    return _column;
  }

  public boolean isAsc() {
    return _isAsc;
  }

  public String toString() {
    if (_isAsc) {
      return "{Column=" + _column + ", Order=Ascending}";
    } else {
      return "{Column=" + _column + ", Order=Descending}";
    }

  }
}
