package com.linkedin.pinot.core.indexsegment.tools;

import java.awt.BorderLayout;
import java.io.File;
import java.io.IOException;

import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import org.apache.commons.configuration.ConfigurationException;

import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegment;
import com.linkedin.pinot.core.indexsegment.columnar.SegmentLoader;

public class PagingTester extends JFrame {
  public PagingTester() throws ConfigurationException, IOException {
    super("Paged JTable Test");
    setSize(300, 200);
    setDefaultCloseOperation(EXIT_ON_CLOSE);
    File indexDir = new File("/home/dpatel/experiments/pinot/index-main/sampleDir");
    ColumnarSegment s = (ColumnarSegment) SegmentLoader.loadMmap(indexDir);
    
    PagedUnsortedArrayTable pm = new PagedUnsortedArrayTable(s.getIntArrayFor("pageKey"));
    JTable jt = new JTable(pm);

    JScrollPane jsp = PagedUnsortedArrayTable.createPagingScrollPaneForTable(jt);
    getContentPane().add(jsp, BorderLayout.CENTER);
  }

  public static void main(String args[]) throws ConfigurationException, IOException {
    PagingTester pt = new PagingTester();
    pt.setVisible(true);
  }
}
