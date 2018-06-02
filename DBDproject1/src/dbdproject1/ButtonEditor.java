package dbdproject1;


import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.table.*;

/**
 * @version 1.0 11/09/98
 */
public class ButtonEditor extends DefaultCellEditor {

    protected JButton button;
    private String label;
    private boolean isPushed;
    DBDproject1 sb = new DBDproject1();

    public ButtonEditor(JCheckBox jc) {
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
        button.setText("Check Out");
        String isbn = (String) table.getValueAt(row, 0);
        String book = (String) table.getValueAt(row, 1);
        if (sb.checkAvail(isbn)) {
            //book is available
            String card_id = JOptionPane.showInputDialog("Please enter Borrower Card ID:");
            sb.checkOut(card_id, isbn, book);
        }else{
            JOptionPane.showMessageDialog(null, "Sorry this book has already been checked out.");
        }

        isPushed = true;
        return button;
    }
}
