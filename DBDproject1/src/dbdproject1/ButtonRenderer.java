package dbdproject1;

import java.awt.*;
import javax.swing.*;
import javax.swing.table.*;
import javax.swing.table.TableCellRenderer;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Jaahanavee Sikri
 */
public class ButtonRenderer extends JButton implements TableCellRenderer {
  
  public ButtonRenderer() {
    setOpaque(true);
  }
   
  @Override
  public Component getTableCellRendererComponent(JTable table, Object value,
                   boolean isSelected, boolean hasFocus, int row, int column) {
    if (isSelected) {
      setForeground(table.getSelectionForeground());
      setBackground(table.getSelectionBackground());
    } else{
      setForeground(table.getForeground());
      setBackground(UIManager.getColor("Button.background"));
    }
    setText("Check Out");
    return this;
  }
}