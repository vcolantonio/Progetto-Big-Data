package flickr.gui.panels

import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.block.BlockBorder
import org.jfree.chart.plot.CategoryPlot
import org.jfree.chart.renderer.category.BarRenderer
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.data.category.{CategoryDataset, DefaultCategoryDataset}

import java.awt.{Color, Dimension}
import javax.swing.JPanel

/*
TODO
    Codice in parte adattato da tutorial per JFreeChart
    al link


 */
object BarChart {

  def createPanel(events: Array[(String, Double)]): JPanel = {
    val chart = createChart(createDataset(events))
    val chartPanel = new ChartPanel(chart, false)
    chartPanel.setFillZoomRectangle(true)
    chartPanel.setMouseWheelEnabled(true)
    chartPanel.setPreferredSize(new Dimension(1000, 400))
    chartPanel
  }

  private def createDataset(events: Array[(String, Double)]) = {
    val dataset = new DefaultCategoryDataset
    events.foreach(x => dataset.addValue(x._2, x._1, ""))
    dataset
  }

  private def createChart(dataset: CategoryDataset) = {
    val chart = ChartFactory.createBarChart("", "username", "score", dataset)

    chart.setBackgroundPaint(Color.WHITE)
    val plot = chart.getPlot.asInstanceOf[CategoryPlot]

    val rangeAxis = plot.getRangeAxis.asInstanceOf[NumberAxis]
    rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits)
    val renderer = plot.getRenderer.asInstanceOf[BarRenderer]
    renderer.setDrawBarOutline(false)
    chart.getLegend.setFrame(BlockBorder.NONE)
    chart
  }


}
