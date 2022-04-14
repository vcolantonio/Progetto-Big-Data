package flickr.gui.panels

import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.data.general.{DefaultPieDataset, PieDataset}

import java.awt.Color
import javax.swing.JPanel

/*
TODO
    Codice in parte adattato da tutorial per JFreeChart
    al link


 */
object PieChart {

  def createPanel(events: Array[(String, Int)]): JPanel = {
    val chart = createChart(createDataset(events))
    val panel = new ChartPanel(chart, false)
    panel.setFillZoomRectangle(true)
    panel.setBackground(new Color(0, 0, 0, 0))
    panel.setMouseWheelEnabled(true)
    panel
  }

  def createChart(dataset: PieDataset[String]) = {
    val chart = ChartFactory.createPieChart("Tags per utilizzo",
      dataset,
      false,
      true,
      false)

    chart
  }

  def createDataset(events: Array[(String, Int)]) = {

    val dataset = new DefaultPieDataset[String]()
    val tot = events.map(x => x._2).reduce((x, y) => x + y)
    val perc = events.map(x => (x._1, (1.0 * x._2) / tot, x._2))
    val other = perc.filter(x => x._2 < 0.005).map(x => x._3).reduce((x, y) => x + y)

    perc.filter(x => x._2 >= 0.005).foreach(x => dataset.setValue(x._1, x._3))
    dataset.setValue("OTHER", other)

    dataset
  }

}
