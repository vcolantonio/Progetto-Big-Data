package flickr.gui.inputforms

import java.awt.{Dimension, GridLayout}
import javax.swing.{JLabel, JPanel, JTextField}

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/GUIForms/JPanel.java to edit this template
 */

class TextInput() extends JPanel {

  var jLabel1: JLabel = null
  var jTextField1: JTextField = null

  initComponents()

  /**
   * This method is called from within the constructor to initialize the form.
   * WARNING: Do NOT modify this code. The content of this method is always
   * regenerated by the Form Editor.
   */
  @SuppressWarnings(Array("unchecked")) private // <editor-fold defaultstate="collapsed" desc="Generated Code">
  def initComponents(): Unit = {
    jLabel1 = new JLabel
    jTextField1 = new JTextField

    setPreferredSize(new Dimension(760, 80))
    setLayout(new GridLayout(2, 2, 20, 0))
    jLabel1.setHorizontalAlignment(javax.swing.SwingConstants.CENTER)

    add(jLabel1)
    add(new JLabel())
    add(jTextField1)
    add(new JLabel())


    // </editor-fold>                        }
    // Variables declaration - do not modify

    // End of variables declaration
  }

}