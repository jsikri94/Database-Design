package dbdproject1;


import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Jaahanavee Sikri
 */
public class BE extends DefaultCellEditor {

    protected JButton button;
    private String label;
    private boolean isPushed;
    DBDproject1 sb = new DBDproject1();
    
    public BE(JCheckBox jc) {
        super(jc);
        button = new JButton();
        button.setOpaque(true);
        button.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
        //fireEditingStopped();
                //System.out.println("Button pressed");  
            }
        });
    }
    
     @Override
    public Component getTableCellEditorComponent(JTable table, Object value,
            boolean isSelected, int row, int column) {
        if (isSelected) {
            button.setForeground(table.getSelectionForeground());
            button.setBackground(table.getSelectionBackground());
            //System.out.println(row);
        } else {
            button.setForeground(table.getForeground());
            button.setBackground(table.getBackground());

        }
        button.setText("Pay");
        String returned = (String) table.getValueAt(row, 3);
        String card_id = (String) table.getValueAt(row, 0);
        if(returned.equalsIgnoreCase("Yes")){
            //only pay fine for books that have been returned
            sb.payFine(card_id);
        }else{
            JOptionPane.showMessageDialog(null, "Fine cannot be paid. Please return books first!");
        }
        
        isPushed = true;
        return button;
    }
}
