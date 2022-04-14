package flickr.gui.panels

import org.apache.spark.sql.Row

import java.awt.datatransfer.StringSelection
import java.awt.event.{MouseAdapter, MouseEvent}
import java.awt.{Color, Dimension, Toolkit}
import javax.swing._
import javax.swing.table.DefaultTableModel


class Table(columns: Array[String], rows: Array[Row]) extends JPanel {

  val defModel = new DefaultTableModel()
  columns.foreach(x => defModel.addColumn(x))

  val table: JTable = new JTable(defModel)

  table.setEnabled(false)
  table.addMouseListener(new MouseAdapter {
    override def mousePressed(mouseEvent: MouseEvent): Unit = {
      val clipboard = Toolkit.getDefaultToolkit.getSystemClipboard
      val row = table.rowAtPoint(mouseEvent.getPoint)
      val col = table.columnAtPoint(mouseEvent.getPoint)

      val stringSelection = new StringSelection(table.getModel.getValueAt(row, col).toString)
      clipboard.setContents(stringSelection, null)
    }

  })
  table.getTableHeader.setReorderingAllowed(false)

  val model: DefaultTableModel = table.getModel.asInstanceOf[DefaultTableModel]

  rows.foreach(x => {
    val z = columns.map(y => {
      x.get(x.fieldIndex(y)).toString
    }).toArray

    model.addRow(z.asInstanceOf[Array[AnyRef]])
  })

  val scrollPane = new JScrollPane(table)
  table.setShowVerticalLines(true)
  table.setShowHorizontalLines(true)
  table.setGridColor(Color.GRAY)

  scrollPane.setPreferredSize(new Dimension(1000, 400))

  add(scrollPane)


}