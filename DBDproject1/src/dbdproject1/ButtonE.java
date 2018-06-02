package dbdproject1;


import java.awt.*;
import java.awt.event.*;
import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;
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
public class ButtonE extends DefaultCellEditor {

    protected JButton button;
    private String label;
    private boolean isPushed;
    DBDproject1 sb = new DBDproject1();
    
    public ButtonE(JCheckBox jc) {
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
        button.setText("Check In");
        String loan_id = (String) table.getValueAt(row, 0);
        String date_in = (String) table.getValueAt(row,5);
        if(date_in==null){
            try {
                sb.checkIn(loan_id);
            } catch (ParseException ex) {
                Logger.getLogger(ButtonE.class.getName()).log(Level.SEVERE, null, ex);
            }
        }else{
            JOptionPane.showMessageDialog(null, "The book has already been checked in!");
        }
        isPushed = true;
        return button;
    }
}
