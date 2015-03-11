/*
 * Copyright (c) 1995, 2008, Oracle and/or its affiliates. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Oracle or the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.linkedin.thirdeye.tools;

import java.awt.Dimension;
import java.awt.GridLayout;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTree;
import javax.swing.UIManager;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.event.TreeWillExpandListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.ExpandVetoException;
import javax.swing.tree.TreeSelectionModel;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeUtils;

public class StarTreeIndexViewer extends JPanel implements
    TreeSelectionListener, TreeWillExpandListener {
  private JEditorPane htmlPane;
  private JTree tree;
  private URL helpURL;
  private static boolean DEBUG = false;

  // Optionally play with line styles. Possible values are
  // "Angled" (the default), "Horizontal", and "None".
  private static boolean playWithLineStyle = false;
  private static String lineStyle = "Horizontal";

  // Optionally set the look and feel.
  private static boolean useSystemLookAndFeel = false;
  private final StarTree _starTree;
  private final StarTreeConfig _config;
  private JEditorPane searchResultPane;
  private final String dataDir;
  private final int numTimeBuckets;

  public StarTreeIndexViewer(StarTreeConfig config, StarTree starTree,
      String dataDir, int numTimeBuckets) {
    super(new GridLayout(1, 0));
    _config = config;
    _starTree = starTree;
    // setLayout(new FlowLayout());
    // Create the nodes.
    this.dataDir = dataDir;
    this.numTimeBuckets = numTimeBuckets;

    DefaultMutableTreeNode top = createNodes();

    // Create a tree that allows one selection at a time.
    tree = new JTree(top);
    tree.getSelectionModel().setSelectionMode(
        TreeSelectionModel.SINGLE_TREE_SELECTION);

    // Listen for when the selection changes.
    tree.addTreeSelectionListener(this);
    tree.addTreeWillExpandListener(this);
    if (playWithLineStyle) {
      System.out.println("line style = " + lineStyle);
      tree.putClientProperty("JTree.lineStyle", lineStyle);
    }
    JTabbedPane tabbedPane = new JTabbedPane();

    // Create the scroll pane and add the tree to it.
    JScrollPane treeView = new JScrollPane(tree);

    // Create the HTML viewing pane.
    htmlPane = new JEditorPane();
    htmlPane.setEditable(false);
    // initHelp();
    JScrollPane htmlView = new JScrollPane(htmlPane);

    // Add the scroll panes to a split pane.
    JSplitPane viewSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
    viewSplitPane.setTopComponent(treeView);
    viewSplitPane.setBottomComponent(htmlView);

    Dimension minimumSize = new Dimension(100, 50);
    htmlView.setMinimumSize(minimumSize);
    treeView.setMinimumSize(minimumSize);
    viewSplitPane.setDividerLocation(100);
    viewSplitPane.setPreferredSize(new Dimension(500, 300));

    // Add the split pane to this panel.
    add(viewSplitPane);

  }

  /** Required by TreeSelectionListener interface. */
  public void valueChanged(TreeSelectionEvent e) {
    DefaultMutableTreeNode node = (DefaultMutableTreeNode) tree
        .getLastSelectedPathComponent();

    if (node == null)
      return;

    NodeInfo nodeInfo = (NodeInfo) node.getUserObject();
    displayNode(nodeInfo);
    if (DEBUG) {
      System.out.println(nodeInfo.toString());
    }
  }

  private void displayNode(NodeInfo nodeInfo) {
    try {
      StringBuffer sb = new StringBuffer("<html><table border=1>");

      String nodeId = nodeInfo._node.getId().toString();
      Map<String, Map<String, Integer>> forwardIndex = StarTreePersistanceUtil
          .readForwardIndex(nodeId, dataDir);
      Map<String, Map<Integer, String>> reverseIndex = StarTreeUtils
          .toReverseIndex(forwardIndex);
      int numMetrics = _config.getMetrics().size();

      int numDimensions = _config.getDimensions().size();
      Map<int[], Map<Long, Number[]>> leafRecords = StarTreePersistanceUtil
          .readLeafRecords(dataDir, nodeId, numDimensions, numMetrics, _config.getMetrics(),
              numTimeBuckets);
      int numColumns = numDimensions + numMetrics + 1;
      int rowId = 0;
      Number[] emptyMetrics = new Number[numMetrics];
      Arrays.fill(emptyMetrics, 0);
      for (Entry<int[], Map<Long, Number[]>> entry : leafRecords.entrySet()) {
        int[] dimArr = entry.getKey();
        sb.append("<tr>");
        for (int i = 0; i < numDimensions; i++) {
          sb.append("<td>");
          sb.append(reverseIndex.get(_config.getDimensions().get(i)).get(
              dimArr[i]));
          sb.append("</td>");
        }
        sb.append("<td colspan=" + (numMetrics + 1) + ">");
        sb.append("</tr>");
        Map<Long, Number[]> timeSeries = entry.getValue();
        if (timeSeries.size() > 0) {
          int count = 0;
          for (Entry<Long, Number[]> timeSeriesEntry : timeSeries.entrySet()) {
            Number[] metrics = timeSeriesEntry.getValue();

            if (timeSeriesEntry.getKey() > 0
                && !Arrays.equals(emptyMetrics, metrics)) {
              sb.append("<tr>");
              sb.append("<td colspan=" + (numDimensions) + ">");
              sb.append("<td>");
              sb.append(timeSeriesEntry.getKey());
              sb.append("</td>");
              for (int i = 0; i < numMetrics; i++) {
                sb.append("<td>");
                sb.append(metrics[i]);
                sb.append("</td>");
              }
              sb.append("</tr>");
              count++;
            }
            if(count >0){
              break;
            }
          }
        }

      }
      htmlPane.setContentType("text/html");
      sb.append("</table></html>");
      htmlPane.setText(sb.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private class NodeInfo {
    public String nodeName;
    private String _attrName;
    private String _attrType;
    private final StarTreeNode _node;

    public NodeInfo(StarTreeNode node) {
      _node = node;
      _attrName = _node.getDimensionName();
      _attrType = _node.getDimensionValue();

    }

    public String toString() {
      return _attrName + ":" + _attrType;
    }
  }

  private DefaultMutableTreeNode createNodes() {
    Queue<DefaultMutableTreeNode> q = new LinkedList<DefaultMutableTreeNode>();
    DefaultMutableTreeNode top = new DefaultMutableTreeNode(new NodeInfo(
        _starTree.getRoot()));
    q.add(top);
    while (!q.isEmpty()) {
      DefaultMutableTreeNode parent = q.remove();
      NodeInfo userObject = (NodeInfo) parent.getUserObject();
      StarTreeNode indexNode = userObject._node;
      if (indexNode.isLeaf()) {
        continue;
      }
      Collection<StarTreeNode> childSet = indexNode.getChildren();
      for (StarTreeNode node : childSet) {
        DefaultMutableTreeNode child = new DefaultMutableTreeNode(new NodeInfo(
            node));
        q.add(child);
        parent.add(child);
      }
      StarTreeNode starChildNode = indexNode.getStarNode();
      if (starChildNode != null) {
        DefaultMutableTreeNode child = new DefaultMutableTreeNode(new NodeInfo(
            starChildNode));
        q.add(child);
        parent.add(child);
      }
    }
    return top;
  }

  /**
   * Create the GUI and show it. For thread safety, this method should be
   * invoked from the event dispatch thread.
   *
   * @param dataDirectory
   */
  public static void createAndShowGUI(StarTreeConfig config, StarTree index,
      String dataDirectory, int numTimeBuckets) {
    if (useSystemLookAndFeel) {
      try {
        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
      } catch (Exception e) {
        System.err.println("Couldn't use system look and feel.");
      }
    }

    // Create and set up the window.
    JFrame frame = new JFrame("Index Tree View");
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

    // Add content to the window.
    frame.add(new StarTreeIndexViewer(config, index, dataDirectory,
        numTimeBuckets));

    // Display the window.
    frame.pack();
    frame.setVisible(true);
  }

  public static void main(String[] args) throws Exception {
    String config = args[0];
    String pathToTreeBinary = args[1];
    final String dataDirectory = args[2];
    final int numTimeBuckets = Integer.parseInt(args[3]);
    final StarTreeConfig starTreeConfig = StarTreeConfig.decode(new FileInputStream(config));

    StarTreeNode starTreeRootNode = StarTreePersistanceUtil
        .loadStarTree(new FileInputStream(pathToTreeBinary));
    final StarTree starTree = new StarTreeImpl(starTreeConfig);

    // Schedule a job for the event dispatch thread:
    // creating and showing this application's GUI.
    javax.swing.SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        createAndShowGUI(starTreeConfig, starTree, dataDirectory,
            numTimeBuckets);
      }
    });
  }

  @Override
  public void treeWillCollapse(TreeExpansionEvent event)
      throws ExpandVetoException {
    System.out.println("Collapsing" + event.getPath());

  }

  @Override
  public void treeWillExpand(TreeExpansionEvent event)
      throws ExpandVetoException {
    System.out.println("Expnding:" + event.getPath());
  }
}
