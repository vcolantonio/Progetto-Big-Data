package flickr.gui.panels

import java.awt.Dimension
import java.io.File
import javax.swing.{BoxLayout, JPanel}

class ImagePanel(filename: String) extends JPanel {

  import java.awt.image.BufferedImage
  import javax.imageio.ImageIO
  import javax.swing.{ImageIcon, JLabel}

  val img: BufferedImage = ImageIO.read(new File(filename))
  val picLabel = new JLabel(new ImageIcon(img))

  setLayout(new BoxLayout(this, BoxLayout.X_AXIS));

  picLabel.setMinimumSize(new Dimension(600, 600))
  add(picLabel)

}
