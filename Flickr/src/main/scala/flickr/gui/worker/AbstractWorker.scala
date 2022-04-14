package flickr.gui.worker

import flickr.gui.FlickrUI

import java.awt.Dimension
import javax.swing.{BoxLayout, JFrame, JPanel, SwingWorker}

abstract class AbstractWorker(flickrUI: FlickrUI) extends SwingWorker[Unit, Unit] with Serializable {
  protected val workerFrame = new JFrame
  protected val workerPanel = new JPanel

  flickrUI.jTree1.setEnabled(false)
  flickrUI.execute.setEnabled(false)

  workerPanel.setLayout(new BoxLayout(workerPanel, BoxLayout.Y_AXIS));

  workerPanel.setSize(new Dimension(1200, 800))

  override def done(): Unit = {
    super.done()

    flickrUI.jTree1.setEnabled(true)
    flickrUI.execute.setEnabled(true)


    workerFrame.setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE)
    workerFrame.pack()

    workerFrame.setResizable(false)

    workerFrame.setVisible(true)
  }
}
