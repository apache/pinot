package com.linkedin.pinot.query.request;

import org.json.JSONObject;


public class Selection {
  private String[] _selectionColumns = null;
  private String[] _orderBySequence = null;
  private int _startDocId;
  private int _endDocId;

  public String[] getSelectionColumns() {
    return _selectionColumns;
  }

  public void setSelectionColumns(String[] selectionColumns) {
    _selectionColumns = selectionColumns;
  }

  public String[] getOrderBySequence() {
    return _orderBySequence;
  }

  public void setOrderBySequence(String[] orderBySequence) {
    _orderBySequence = orderBySequence;
  }

  public int getStartDocId() {
    return _startDocId;
  }

  public void setStartDocId(int startDocId) {
    _startDocId = startDocId;
  }

  public int getEndDocId() {
    return _endDocId;
  }

  public void setEndDocId(int endDocId) {
    _endDocId = endDocId;
  }

  public static Selection fromJson(JSONObject jsonObject) {
    Selection selection = new Selection();
    selection.setSelectionColumns(jsonObject.getString("selectionColumn").split(","));
    selection.setOrderBySequence(jsonObject.getString("orderByColumn").split(","));
    selection.setStartDocId(jsonObject.getInt("startDocId"));
    selection.setEndDocId(jsonObject.getInt("endDocId"));
    return selection;
  }

}
