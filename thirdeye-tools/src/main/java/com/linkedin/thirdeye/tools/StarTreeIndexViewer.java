package com.linkedin.thirdeye.tools;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeSelectionModel;

import com.linkedin.thirdeye.api.StarTree;

public class StarTreeIndexViewer {

  public static void main(String[] args) {

  }
}

class TreePanel extends JPanel implements ActionListener {

  private DefaultMutableTreeNode rootNode;
  private DefaultTreeModel treeModel;
  private JTree tree;

  public TreePanel(StarTree starTree) {
    super(new BorderLayout());

    // Create the components.
    populateTree(starTree);

    // JButton addButton = new JButton("Collapse All");
    // addButton.setActionCommand(COLLAPSE_ALL_COMMAND);
    // addButton.addActionListener(this);
    //
    // JButton removeButton = new JButton("Show Leaf Content");
    // removeButton.setActionCommand(SHOW_LEAF_COMMAND);
    // removeButton.addActionListener(this);

    // Lay everything out.
    rootNode = new DefaultMutableTreeNode("Root Node");
    treeModel = new DefaultTreeModel(rootNode);

    tree = new JTree(treeModel);
    tree.setEditable(true);
    tree.getSelectionModel().setSelectionMode
            (TreeSelectionModel.SINGLE_TREE_SELECTION);
    tree.setShowsRootHandles(true);

    JScrollPane scrollPane = new JScrollPane(tree);
    add(scrollPane);
    tree.setPreferredSize(new Dimension(800, 600));
    add(tree, BorderLayout.CENTER);

    JPanel panel = new JPanel(new GridLayout(0, 3));
//    panel.add(addButton);
//    panel.add(removeButton);
//    panel.add(clearButton);
    add(panel, BorderLayout.SOUTH);
  }

  private void populateTree(StarTree starTree) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void actionPerformed(ActionEvent e) {
    // TODO Auto-generated method stub
    
  }
}

class LeafRecordPanel {

}