package flickr.gui.inputforms

import net.sourceforge.jdatepicker.JDateComponentFactory

import java.awt.{Dimension, GridLayout}
import javax.swing.{JLabel, JPanel}

class Date1Date2() extends JPanel {

  val componentFactory = new JDateComponentFactory

  import net.sourceforge.jdatepicker.impl.{JDatePanelImpl, JDatePickerImpl, UtilDateModel}

  val model1 = new UtilDateModel
  val datePanel1 = new JDatePanelImpl(model1)
  val jDateChooser1 = new JDatePickerImpl(datePanel1)
  datePanel1.setPreferredSize(new Dimension(400, 200))

  val model2 = new UtilDateModel
  val datePanel2 = new JDatePanelImpl(model2)
  var jDateChooser2 = new JDatePickerImpl(datePanel2)
  datePanel2.setPreferredSize(new Dimension(400, 200))

  var jLabel1: JLabel = null
  var jLabel2: JLabel = null

  initComponents()

  /**
   * This method is called from within the constructor to initialize the form.
   * WARNING: Do NOT modify this code. The content of this method is always
   * regenerated by the Form Editor.
   */
  @SuppressWarnings(Array("unchecked")) private // <editor-fold defaultstate="collapsed" desc="Generated Code">
  def initComponents(): Unit = {

    jLabel1 = new JLabel
    jLabel2 = new JLabel

    jDateChooser1.setShowYearButtons(true)
    jDateChooser2.setShowYearButtons(true)

    setPreferredSize(new Dimension(760, 80))
    setLayout(new GridLayout(2, 2, 20, 0))
    jLabel1.setHorizontalAlignment(javax.swing.SwingConstants.CENTER)
    jLabel1.setText("Data di inizio")
    add(jLabel1)
    jLabel2.setHorizontalAlignment(javax.swing.SwingConstants.CENTER)
    jLabel2.setText("Data di fine")
    add(jLabel2)

    add(jDateChooser1)
    add(jDateChooser2)
  }

}