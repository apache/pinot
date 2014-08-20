package com.linkedin.pinot.core.indexsegment.tools;

import java.awt.Dimension;
import java.io.File;
import java.io.IOException;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.UIManager;
import javax.swing.UIManager.LookAndFeelInfo;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadata;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;
import com.linkedin.pinot.core.indexsegment.utils.SortedIntArray;


/**
 * 
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 19, 2014
 */
public class IndexViewer implements ListSelectionListener {
  protected static final Logger logger = LoggerFactory.getLogger(IndexViewer.class);

  static {
    try {
      for (LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
        if ("Nimbus".equals(info.getName())) {
          UIManager.setLookAndFeel(info.getClassName());
          break;
        }
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  private ColumnarSegment segment;
  private String[] entries;
  private JList list;
  private JScrollPane indexFilesListPane;
  private JScrollPane indexViewerPane;
  private JSplitPane splitPane;
  private File indexDir;
  
  /**
   * 
   * @param indexDir
   * @throws ConfigurationException
   * @throws IOException
   */
  public IndexViewer(File indexDir) throws ConfigurationException, IOException {
    this.indexDir = indexDir;
    segment = (ColumnarSegment) ColumnarSegmentLoader.loadMmap(indexDir);
    entries = new String[indexDir.listFiles().length];
    int i = 0;
    for (File f : indexDir.listFiles()) {
      entries[i] = f.getName();
      i++;
    }
    list = new JList(entries);
    list.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    list.setSelectedIndex(0);
    list.addListSelectionListener(this);

    indexFilesListPane = new JScrollPane(list);
    indexViewerPane =
        new JScrollPane(getUnsortedTableFor(segment.getColumnMetadataMap().entrySet().iterator().next().getKey()));
    splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, indexFilesListPane, indexViewerPane);

    splitPane.setPreferredSize(new Dimension(1200, 1000));
  }

  private JTable getUnsortedTableFor(String column) {
    return new JTable(new PagedUnsortedArrayTable(getIntArrayFor(column)));
  }

  private JTable getSortedTableFor(String column) {
    return new JTable(new SortedForwardIndexTable((SortedIntArray) segment.getIntArrayFor(column)));
  }

  private JTable getDictTableFor(String column) {
    return new JTable(new PagedDictionaryTable(segment.getDictionaryFor(column)));
  }

  private JTable getMetadataTableFor() {
    return new JTable(new MetadataTable((ColumnarSegmentMetadata) segment.getSegmentMetadata()));
  }

  public IntArray getIntArrayFor(String name) {
    IntArray a = segment.getIntArrayFor(name);
    return a;
  }

  public JSplitPane getSplitPane() {
    return splitPane;
  }

  private static void createAndShowGUI(File indexDir) throws ConfigurationException, IOException {
    JFrame frame = new JFrame("IndexViewer");
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    IndexViewer splitPaneDemo = new IndexViewer(indexDir);
    frame.getContentPane().add(splitPaneDemo.getSplitPane());
    frame.pack();
    frame.setVisible(true);
  }

  @Override
  public void valueChanged(ListSelectionEvent e) {
    JList list = (JList) e.getSource();
    System.out.println("selected Index : " + list.getSelectedIndex() + " corresponds to : "
        + entries[list.getSelectedIndex()]);
    String selection = entries[list.getSelectedIndex()];
    if (selection.endsWith("unSorted")) {
      System.out.println(selection.indexOf("."));
      System.out.println(selection.substring(0, selection.indexOf(".")));
      String columnName = selection.substring(0, selection.indexOf("."));
      JTable t = getUnsortedTableFor(columnName);
      indexViewerPane = PagedUnsortedArrayTable.createPagingScrollPaneForTable(t);
      splitPane.setRightComponent(indexViewerPane);
    }
    if (selection.endsWith("dict")) {
      String columnName = selection.substring(0, selection.indexOf("."));
      indexViewerPane = PagedDictionaryTable.createPagingScrollPaneForTable(getDictTableFor(columnName));
      splitPane.setRightComponent(indexViewerPane);
    }
    if (selection.endsWith("sorted")) {
      String columnName = selection.substring(0, selection.indexOf("."));
      indexViewerPane = new JScrollPane(getSortedTableFor(columnName));
      splitPane.setRightComponent(indexViewerPane);
    }

    if (selection.endsWith("properties")) {
      indexViewerPane = new JScrollPane(getMetadataTableFor());
      splitPane.setRightComponent(indexViewerPane);
    }

    if (selection.endsWith("vr")) {
      try {
        SegmentVersion v = ColumnarSegmentLoader.extractVersion(indexDir);
        JLabel l = new JLabel(v.toString());
        indexViewerPane = new JScrollPane(l);
        splitPane.setRightComponent(indexViewerPane);
      } catch (IOException e1) {
        logger.error(e1.getMessage());
      }
    }
  }

  public static void main(String[] args) {
    final File indexDir = new File("/home/dpatel/experiments/pinot/index-main/sampleDir");
    javax.swing.SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        try {
          createAndShowGUI(indexDir);
        } catch (Exception e) {
          logger.error(e.getMessage());
        }
      }
    });
  }
}
