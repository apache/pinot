package com.linkedin.pinot.core.indexsegment.tools;

import java.io.File;

import javax.swing.JFrame;

import com.linkedin.pinot.core.chunk.index.ColumnarChunk;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;


public class PagingTester extends JFrame {
  public PagingTester() throws Exception {
    super("Paged JTable Test");
    setSize(300, 200);
    setDefaultCloseOperation(EXIT_ON_CLOSE);
    final File indexDir = new File("/home/dpatel/experiments/pinot/index-main/sampleDir");
    final ColumnarChunk s = (ColumnarChunk) ColumnarSegmentLoader.loadMmap(indexDir);

    //final PagedUnsortedArrayTable pm = new PagedUnsortedArrayTable(s.getIntArrayFor("pageKey"));
    //final JTable jt = new JTable(pm);

    //inal JScrollPane jsp = PagedUnsortedArrayTable.createPagingScrollPaneForTable(jt);
    //getContentPane().add(jsp, BorderLayout.CENTER);
  }

  public static void main(String args[]) throws Exception {
    final PagingTester pt = new PagingTester();
    pt.setVisible(true);
  }
}
