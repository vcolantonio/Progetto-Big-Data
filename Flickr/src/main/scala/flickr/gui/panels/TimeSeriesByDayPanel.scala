package flickr.gui.panels

import org.jfree.chart.axis.DateAxis
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.data.time.{Day, TimeSeries, TimeSeriesCollection}
import org.jfree.data.xy.XYDataset

import java.awt.Color
import java.text.SimpleDateFormat
import java.time.LocalDate
import javax.swing.JPanel

/*
TODO
    Codice in parte adattato da tutorial per JFreeChart
    al link


 */
object TimeSeriesByDayPanel {

  def createPanel(events: Array[(LocalDate, Long)], label: String, xlabel: String, ylabel: String): JPanel = {
    val chart = createChart(createDataset(events, label), xlabel, ylabel)
    val panel = new ChartPanel(chart, false)
    panel.setFillZoomRectangle(true)
    panel.setMouseWheelEnabled(true)
    panel
  }

  def createChart(dataset: XYDataset, xlabel: String, ylabel: String) = {
    val chart = ChartFactory.createTimeSeriesChart(null,
      xlabel,
      ylabel,
      dataset)

    val plot = chart.getPlot.asInstanceOf[XYPlot]
    val r = plot.getRenderer
    if (r.isInstanceOf[XYLineAndShapeRenderer]) {
      val renderer = r.asInstanceOf[XYLineAndShapeRenderer]

      renderer.setDrawSeriesLineAsPath(true)
      renderer.setSeriesPaint(0, Color.BLACK)
    }
    val axis = plot.getDomainAxis.asInstanceOf[DateAxis]
    axis.setDateFormatOverride(new SimpleDateFormat("dd-MMM-yyyy"))
    chart
  }

  def createDataset(events: Array[(LocalDate, Long)], label: String) = {
    val s = new TimeSeries(label)
    events.foreach(x => s.add(new Day(
      x._1.getDayOfMonth,
      x._1.getMonthValue,
      x._1.getYear), x._2))

    val dataset = new TimeSeriesCollection
    dataset.addSeries(s)
    dataset
  }

}
