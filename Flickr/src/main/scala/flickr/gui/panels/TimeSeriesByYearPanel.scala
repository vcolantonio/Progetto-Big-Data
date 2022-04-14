package flickr.gui.panels

import org.jfree.chart.axis.DateAxis
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.data.time.{TimeSeries, TimeSeriesCollection, Year}
import org.jfree.data.xy.XYDataset

import java.awt.Color
import java.text.SimpleDateFormat
import javax.swing.JPanel

/*
TODO
    Codice in parte adattato da tutorial per JFreeChart
    al link


 */
object TimeSeriesByYearPanel {

  def createPanel(events: Array[(String, Long)], label: String, xlabel: String, ylabel: String): JPanel = {
    val chart = createChart(createDataset(events, label), xlabel, ylabel)
    val panel = new ChartPanel(chart, false)
    panel.setFillZoomRectangle(true)
    panel.setMouseWheelEnabled(true)
    panel
  }

  def createChart(dataset: XYDataset, xlabel: String, ylabel: String) = {
    val chart = ChartFactory.createTimeSeriesChart(null, // title
      xlabel, // x-axis label
      ylabel, // y-axis label
      dataset)
    val plot = chart.getPlot.asInstanceOf[XYPlot]
    val r = plot.getRenderer
    if (r.isInstanceOf[XYLineAndShapeRenderer]) {
      val renderer = r.asInstanceOf[XYLineAndShapeRenderer]
      renderer.setDefaultShapesVisible(true)
      renderer.setDefaultShapesFilled(true)
      renderer.setDrawSeriesLineAsPath(true)

      r.setSeriesPaint(0, Color.RED) // colore serie 0
    }
    val axis = plot.getDomainAxis.asInstanceOf[DateAxis]
    axis.setDateFormatOverride(new SimpleDateFormat("yyyy"))
    chart
  }

  def createDataset(events: Array[(String, Long)], label: String) = {
    val s = new TimeSeries(label)
    events.foreach(x => s.add((new Year(Integer.parseInt(x._1))), x._2))
    val dataset = new TimeSeriesCollection
    dataset.addSeries(s)
    dataset
  }

}
