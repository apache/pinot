package com.linkedin.pinot.query.request;

import java.util.Arrays;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * Selection specifies what and how the search results should be returned.
 * 
 * @author xiafu
 *
 */
public class Selection {
  private String[] _selectionColumns = null;
  private SelectionSort[] _selectionSortSequence = null;
  private int _offset;
  private int _size;

  public static SelectionSort getAscSelectionSort(String column) {
    SelectionSort sort = new SelectionSort(column, true);
    return sort;
  }

  public static SelectionSort getDescSelectionSort(String field) {
    SelectionSort sort = new SelectionSort(field, false);
    return sort;
  }

  public String[] getSelectionColumns() {
    return _selectionColumns;
  }

  public void setSelectionColumns(String[] selectionColumns) {
    _selectionColumns = selectionColumns;
  }

  public SelectionSort[] getSelectionSortSequence() {
    return _selectionSortSequence;
  }

  public void setSelectionSortSequence(SelectionSort[] selectionSortSequence) {
    _selectionSortSequence = selectionSortSequence;
  }

  public int getOffset() {
    return _offset;
  }

  public void setOffset(int offset) {
    _offset = offset;
  }

  public int getSize() {
    return _size;
  }

  public void setSize(int size) {
    _size = size;
  }

  public String toString() {
    return "{offset=" + _offset + ", size="
        + _size + ", Selction Columns=" + Arrays.toString(_selectionColumns)
        + ", Selection Orders=" + Arrays.toString(_selectionSortSequence) + "}";

  }

  public static Selection fromJson(JSONObject jsonObject) throws JSONException {
    Selection selection = new Selection();
    selection.setSelectionColumns(jsonObject.getString("columns").split(","));

    String[] sortSequences = jsonObject.getString("sorts").split(",");
    SelectionSort[] selectionOrders = new SelectionSort[sortSequences.length];

    for (int i = 0; i < selectionOrders.length; ++i) {
      String seq = sortSequences[i];

      String[] splittedSequences = seq.split(" ");
      if (splittedSequences.length == 2) {
        if (splittedSequences[1].equalsIgnoreCase("desc")) {
          selectionOrders[i] = getDescSelectionSort(splittedSequences[0]);
        }
      } else {
        selectionOrders[i] = getAscSelectionSort(splittedSequences[0]);
      }

    }

    selection.setSelectionSortSequence(selectionOrders);
    selection.setOffset(jsonObject.getInt("offset"));
    selection.setSize(jsonObject.getInt("size"));
    return selection;
  }

  public static enum Order {
    desc,
    asc;
  }

}
