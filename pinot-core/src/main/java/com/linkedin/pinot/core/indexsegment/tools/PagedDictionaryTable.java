package com.linkedin.pinot.core.indexsegment.tools;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ScrollPaneConstants;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableModel;

import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;


public class PagedDictionaryTable extends AbstractTableModel {
  private static final int DEFAULT_PAGE_SIZE = 5000;
  private static final String[] columnNames = { "Dictionary Value", "Original Value" };

  protected int pageSize;
  protected int pageOffset;
  protected Dictionary<?> data;

  public PagedDictionaryTable(Dictionary<?> array) {
    this(array.size() - 1, DEFAULT_PAGE_SIZE, array);
  }

  public PagedDictionaryTable(int numRows, int size, Dictionary<?> array) {
    data = array;
    pageSize = size;
  }

  public int getRowCount() {
    return Math.min(pageSize, data.size());
  }

  public int getColumnCount() {
    return 2;
  }

  public Object getValueAt(int row, int col) {
    int realRow = row + (pageOffset * pageSize);
    if (col == 0)
      return realRow;
    return data.getString(realRow);
  }

  public String getColumnName(int col) {
    return columnNames[col];
  }

  public int getPageOffset() {
    return pageOffset;
  }

  public int getPageCount() {
    return (int) Math.ceil((double) data.size() / pageSize);
  }

  public int getRealRowCount() {
    return data.size();
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageSize(int s) {
    if (s == pageSize) {
      return;
    }
    int oldPageSize = pageSize;
    pageSize = s;
    pageOffset = (oldPageSize * pageOffset) / pageSize;
    fireTableDataChanged();
  }

  public void pageDown() {
    if (pageOffset < getPageCount() - 1) {
      pageOffset++;
      fireTableDataChanged();
    }
  }

  public void pageUp() {
    if (pageOffset > 0) {
      pageOffset--;
      fireTableDataChanged();
    }
  }

  public static JScrollPane createPagingScrollPaneForTable(JTable jt) {
    JScrollPane jsp = new JScrollPane(jt);
    TableModel tmodel = jt.getModel();

    if (!(tmodel instanceof PagedDictionaryTable)) {
      return jsp;
    }

    final PagedDictionaryTable model = (PagedDictionaryTable) tmodel;
    final JButton upButton = new JButton("up");
    upButton.setEnabled(false); // starts off at 0, so can't go up
    final JButton downButton = new JButton("down");
    if (model.getPageCount() <= 1) {
      downButton.setEnabled(false); // One page...can't scroll down
    }

    upButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent ae) {
        model.pageUp();

        if (model.getPageOffset() == 0) {
          upButton.setEnabled(false);
        }
        downButton.setEnabled(true);
      }
    });

    downButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent ae) {
        model.pageDown();

        if (model.getPageOffset() == (model.getPageCount() - 1)) {
          downButton.setEnabled(false);
        }
        upButton.setEnabled(true);
      }
    });

    jsp.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
    jsp.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS);

    jsp.setCorner(ScrollPaneConstants.UPPER_RIGHT_CORNER, upButton);
    jsp.setCorner(ScrollPaneConstants.LOWER_RIGHT_CORNER, downButton);

    return jsp;
  }
}
