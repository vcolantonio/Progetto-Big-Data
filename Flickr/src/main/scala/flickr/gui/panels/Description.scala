package flickr.gui.panels

import java.awt.{BorderLayout, Dimension, Font}
import javax.swing._

class Description() extends JPanel {

  val title = new JLabel
  val description = new JTextArea

  setLayout(new BorderLayout)

  description.setWrapStyleWord(true)
  description.setLineWrap(true)
  description.setEditable(false)

  title.setHorizontalAlignment(SwingConstants.LEFT);
  title.setFont(new Font("", Font.BOLD, 24))

  description.setSize(new Dimension(760, 500))

  add(title, BorderLayout.NORTH)
  add(description, BorderLayout.CENTER)

  add(new JSeparator, BorderLayout.PAGE_END)

  def setTitle(title: String) = this.title.setText(title)

  def setDescription(description: String) = this.description.setText("\n" + description + "\n")

}