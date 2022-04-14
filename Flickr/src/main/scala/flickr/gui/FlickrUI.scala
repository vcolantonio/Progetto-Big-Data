package flickr.gui

import com.formdev.flatlaf.FlatLightLaf
import flickr.gui.inputforms.{ComboInput, Date1Date2, IntInput, TextInput}
import flickr.gui.panels._
import flickr.gui.worker.AbstractWorker
import flickr.query._
import flickr.spark.SparkInitializer.{dataset, spark}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.block.BlockBorder
import org.jfree.chart.plot.CategoryPlot
import org.jfree.chart.renderer.category.BarRenderer
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.data.category.{CategoryDataset, DefaultCategoryDataset}

import java.awt.event.ActionEvent
import java.awt.{BorderLayout, Color, Dimension, Font}
import java.io.File
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.Date
import javax.swing.WindowConstants._
import javax.swing._
import javax.swing.event.{TreeSelectionEvent, TreeSelectionListener}
import javax.swing.tree.DefaultMutableTreeNode

/*
    Codice in parte realizzato in Java dal
     tool automatico Netbeans, adattato in Scala

 */
object FlickrUI extends JFrame {

  def main(args: Array[String]): Unit = {

    try {
      FlatLightLaf.setup
      UIManager.setLookAndFeel(new FlatLightLaf())
      System.setProperty("awt.useSystemAAFontSettings", "lcd")
      System.setProperty("swing.aatext", "true")
    } catch {
      case _: UnsupportedLookAndFeelException =>
        System.err.println("FlatLaf: Unsupported Look and feel!")
    }

    java.awt.EventQueue.invokeLater(new Runnable() {
      override def run(): Unit = {

        new FlickrUI().setVisible(true)
      }
    })
  }
}

class FlickrUI() extends JFrame {

  import spark.implicits._

  val self = this

  val tagQuery = new TagQueries(spark)
  val postQuery = new PostQueries(spark)
  val trajQuery = new TrajectoryQueries(spark)
  val usClQuery = new UserClusteringQueries(spark)
  val mlQuery = new MLQueries(spark)
  val root = new DefaultMutableTreeNode("Query")
  // MlQueries
  val mlQueries = new DefaultMutableTreeNode("1 Query di Machine Learning")
  val mlQueries_1 = new DefaultMutableTreeNode("1.1 Classificazione ImageNet ")
  val mlQueries_2 = new DefaultMutableTreeNode("1.2 Lingua di un post ")
  val mlQueries_1_description: String = "La query fornisce una classificazione ImageNet dell'immagine in input.\n" +
  "Seleziona una immagine tra quelle disponibili in 'images'\n" +
  "Premi poi sul pulsante 'Esegui'.";
  val mlQueries_2_description: String = "La query associa ad un post Flickr la lingua in cui esso è stato scritto (probabilmente).\n" +
  "Inserisci o seleziona l'ID di un post Flickr.\n" +
  "Premi poi sul pulsante 'Esegui'.";

  mlQueries.add(mlQueries_1)
  mlQueries.add(mlQueries_2)
  root.add(mlQueries)
  // PostQueries
  val postQueries = new DefaultMutableTreeNode("2 Query per post di Flickr")
  val postQueries_1 = new DefaultMutableTreeNode("2.1 Utenti più influenti")
  val postQueries_1_1 = new DefaultMutableTreeNode("2.1.1 Totale post")
  val postQueries_1_2 = new DefaultMutableTreeNode("2.1.2 Totale visualizzazioni")
  val postQueries_1_3 = new DefaultMutableTreeNode("2.1.3 Score")
  val postQueries_1_1_description: String = "La query individua alcuni degli utenti più 'influenti'.\n" +
    "La nozione di influenza è qui basata sul numero di post totali.\n" +
  "Premi sul pulsante 'Esegui'.";
  val postQueries_1_2_description: String = "La query individua alcuni degli utenti più 'influenti'.\n" +
    "La nozione di influenza è qui basata sul numero di visualizzazioni totali.\n" +
    "Premi sul pulsante 'Esegui'.";
  val postQueries_1_3_description: String = "La query individua alcuni degli utenti più 'influenti'.\n" +
    "La nozione di influenza è qui basata su una metrica dipendente dal numero di post totali e dalla media di visualizzazioni per post.\n" +
    "Premi sul pulsante 'Esegui'.";
  val postQueries_2 = new DefaultMutableTreeNode("2.2 Serie temporali")
  val postQueries_2_1 = new DefaultMutableTreeNode("2.2.4 Andamento post utente, per anno")
  val postQueries_2_2 = new DefaultMutableTreeNode("2.2.5 Andamento post utente, per mese")
  val postQueries_2_3 = new DefaultMutableTreeNode("2.2.6 Andamento post utente, per giorno")
  val postQueries_2_1_description: String = "La query mostra l'andamento del numero di post per un dato utente nel tempo.\n" +
  "Seleziona o inserisci l'ID dell'utente di interesse.\n" +
  "Premi poi sul pulsante 'Esegui'.";
  val postQueries_2_2_description: String = "La query mostra l'andamento del numero di post per un dato utente nel tempo.\n" +
  "Seleziona o inserisci l'ID dell'utente di interesse.\n" +
  "Premi poi sul pulsante 'Esegui'.";
  val postQueries_2_3_description: String = "La query mostra l'andamento del numero di post per un dato utente nel tempo.\n" +
  "Seleziona o inserisci l'ID dell'utente di interesse.\n" +
  "Premi poi sul pulsante 'Esegui'.";

  val postQueries_2_4 = new DefaultMutableTreeNode("2.2.4 Andamento visualizzazioni utente, per anno")
  val postQueries_2_5 = new DefaultMutableTreeNode("2.2.5 Andamento visualizzazioni utente, per mese")
  val postQueries_2_6 = new DefaultMutableTreeNode("2.2.6 Andamento visualizzazioni utente, per giorno")
  val postQueries_2_4_description: String = "La query mostra l'andamento del numero di visualizzazioni dei post di un dato utente nel tempo.\n" +
    "Seleziona o inserisci l'ID dell'utente di interesse.\n" +
    "Premi poi sul pulsante 'Esegui'.";
  val postQueries_2_5_description: String = "La query mostra l'andamento del numero di visualizzazioni dei post di un dato utente nel tempo.\n" +
    "Seleziona o inserisci l'ID dell'utente di interesse.\n" +
    "Premi poi sul pulsante 'Esegui'.";
  val postQueries_2_6_description: String = "La query mostra l'andamento del numero di visualizzazioni dei post di un dato utente nel tempo.\n" +
    "Seleziona o inserisci l'ID dell'utente di interesse.\n" +
    "Premi poi sul pulsante 'Esegui'.";
  val postQueries_3 = new DefaultMutableTreeNode("2.3 Post utente")

  postQueries.add(postQueries_1)
  postQueries_1.add(postQueries_1_1)
  postQueries_1.add(postQueries_1_2)
  postQueries_1.add(postQueries_1_3)
  val postQueries_3_description: String = "La query mostra alcuni post di un utente.\n" +
    "Seleziona un utente, filtra per date e seleziona il numero massimo di righe da visualizzare.\n" +
    "Premi poi sul pulsante 'Esegui'.";
  // TagQueries
  val tagQueries = new DefaultMutableTreeNode("3 Query per tag dei post")
  val tagQueries_1 = new DefaultMutableTreeNode("3.1 Post dato un tag")
  val tagQueries_1_description: String = "Seleziona o inserisci un tag di interesse.\n" +
    "Verranno mostrate alcune righe del database relative a quel tag.";
  val tagQueries_2 = new DefaultMutableTreeNode("3.2 Serie temporali")
  val tagQueries_2_1 = new DefaultMutableTreeNode("3.2.1 Andamento utilizzo tag, per anno")
  val tagQueries_2_2 = new DefaultMutableTreeNode("3.2.2 Andamento utilizzo tag, per mese")

  postQueries_2.add(postQueries_2_1)
  postQueries_2.add(postQueries_2_2)
  postQueries_2.add(postQueries_2_3)
  val tagQueries_2_3 = new DefaultMutableTreeNode("3.2.3 Andamento utilizzo tag, per giorno")
  val tagQueries_2_1_description: String ="La query mostra l'andamento dell'utilizzo di un tag nel tempo.\n" +
    "Seleziona o inserisci il tag di interesse.\n" +
    "Premi poi sul pulsante 'Esegui'.";
  val tagQueries_2_2_description: String = "La query mostra l'andamento dell'utilizzo di un tag nel tempo.\n" +
    "Seleziona o inserisci il tag di interesse.\n" +
    "Premi poi sul pulsante 'Esegui'.";
  val tagQueries_2_3_description: String = "La query mostra l'andamento dell'utilizzo di un tag nel tempo.\n" +
    "Seleziona o inserisci il tag di interesse.\n" +
    "Premi poi sul pulsante 'Esegui'.";
  val tagQueries_3 = new DefaultMutableTreeNode("3.3 Distribuzione dei tag")
  val tagQueries_3_description: String = "Filtra per date; Premi poi sul pulsante 'Esegui' per ottenere un grafico " +
    "sulla distribuzione dell'utilizzo dei tag in quell'arco temporale"

  postQueries_2.add(postQueries_2_4)
  postQueries_2.add(postQueries_2_5)
  postQueries_2.add(postQueries_2_6)

  postQueries.add(postQueries_2)
  // TrajQueries
  val trajQueries = new DefaultMutableTreeNode("4 Query di Trajectory Mining")
  val trajQueries_1 = new DefaultMutableTreeNode("4.1 PrefixSpan")

  postQueries.add(postQueries_3)
  root.add(postQueries)
  val trajQueries_1_description: String = "La query restituisce i top 'n' luoghi per ogni lunghezza di path possibile." +
    "'n' può essere settato tramite la prima input form. È possibile filtrare le sequenze che contengono un luogo in particolare."
  // UsClusteringQueries
  val usClQueries = new DefaultMutableTreeNode("5 Query di User Clustering")
  val usClQueries_2 = new DefaultMutableTreeNode("5.1 KMeans, k = 6")

  tagQueries.add(tagQueries_1)
  val usClQueries_2_1 = new DefaultMutableTreeNode("5.1.1 Statistiche cluster")
  val usClQueries_2_2 = new DefaultMutableTreeNode("5.1.2 Utenti simili")
  val usClQueries_2_3 = new DefaultMutableTreeNode("5.1.3 Rappresentanti dei cluster")
  val usClQueries_2_1_description: String = "La query mostra un grafico con alcune statistiche di interesse relative ai cluster" +
    " calcolati tramite KMeans, con parametro k=6."
  val usClQueries_2_2_description: String = "La query calcola, dato un utente in input, alcuni degli utenti a lui vicini nello " +
    "spazio latente.\n" +
    "È possibile impostare un limite al numero di utenti simili da mostrare."
  val usClQueries_2_3_description: String = "La query calcola, dato un indice di cluster in input, alcuni dei suoi rappresentanti.\n" +
    "Per rappresentanti si intendono gli utenti più vicini al centro di massa del cluster.";
  var jScrollPane1: JScrollPane = null


  tagQueries.add(tagQueries_2)
  tagQueries_2.add(tagQueries_2_1)
  tagQueries_2.add(tagQueries_2_2)
  tagQueries_2.add(tagQueries_2_3)
  var jScrollPane2: JScrollPane = null
  tagQueries.add(tagQueries_3)
  var jTree1: JTree = null

  root.add(tagQueries)
  var jToolBar1: JToolBar = null
  var jPanel1: JPanel = null
  var jPanel2: JPanel = null

  trajQueries.add(trajQueries_1)
  root.add(trajQueries)
  var jPanel3: JPanel = null
  var jPanel4: JPanel = null
  var jPanel5: JPanel = null
  var jPanel6: JPanel = null
  var execute: JButton = new JButton("Esegui")
  var jSplitPane1: JSplitPane = null
  var jProgressBar1: JProgressBar = null
  var jTextArea1: JTextArea = null

  usClQueries_2.add(usClQueries_2_1)
  usClQueries_2.add(usClQueries_2_2)
  usClQueries_2.add(usClQueries_2_3)

  usClQueries.add(usClQueries_2)

  root.add(usClQueries)

  initComponents()

  def initComponents(): Unit = {
    this.setGlobalFont(new javax.swing.plaf.FontUIResource(new Font(Font.DIALOG, Font.PLAIN, 16)))

    jToolBar1 = new JToolBar

    jProgressBar1 = new JProgressBar
    jPanel1 = new JPanel
    jPanel2 = new JPanel
    jPanel3 = new JPanel
    jPanel4 = new JPanel
    jPanel5 = new JPanel
    jPanel6 = new JPanel

    jSplitPane1 = new JSplitPane
    jScrollPane1 = new JScrollPane
    jScrollPane2 = new JScrollPane
    jTree1 = new JTree(root)
    jTextArea1 = new JTextArea

    setDefaultCloseOperation(EXIT_ON_CLOSE)

    jToolBar1.setRollover(true)

    jPanel3.setLayout(new BorderLayout)

    jProgressBar1.setMinimumSize(new Dimension(300, 5))
    jPanel3.add(jTextArea1, java.awt.BorderLayout.NORTH)
    jPanel3.add(jProgressBar1, java.awt.BorderLayout.SOUTH)

    jToolBar1.add(jPanel3)

    getContentPane.add(jToolBar1, java.awt.BorderLayout.PAGE_END)

    jPanel1.setLayout(new BorderLayout)

    jScrollPane1.setPreferredSize(new Dimension(500, 400))

    jTree1.setEditable(false)
    jTree1.addTreeSelectionListener(new TreeSelectionListener() {
      override def valueChanged(evt: TreeSelectionEvent): Unit = {
        jTree1ValueChanged(evt)
      }
    })
    jScrollPane1.setViewportView(jTree1)

    jPanel1.add(jScrollPane1, java.awt.BorderLayout.CENTER)

    jSplitPane1.setLeftComponent(jPanel1)
    jSplitPane1.setEnabled(false)

    jPanel6.setLayout(new BorderLayout)
    jPanel6.add(new JLabel, BorderLayout.CENTER)
    val jPanel7 = new JPanel
    jPanel7.add(execute)
    jPanel7.add(new JLabel(" "))
    jPanel6.add(jPanel7, BorderLayout.EAST)
    execute.setPreferredSize(new Dimension(100, 40))

    jPanel5.setLayout(new BorderLayout)
    jPanel5.setPreferredSize(new Dimension(900, 740))
    jPanel1.setPreferredSize(new Dimension(500, 740))

    jPanel5.add(jPanel2, BorderLayout.CENTER)
    jPanel5.add(jPanel6, BorderLayout.PAGE_END)

    jSplitPane1.setRightComponent(jPanel5)

    getContentPane.add(jSplitPane1, java.awt.BorderLayout.CENTER)

    jTextArea1.setWrapStyleWord(true)
    jTextArea1.setLineWrap(true)
    jTextArea1.setEditable(false)

    setResizable(false)
    setSize(new Dimension(1400, 800))

    pack()

  }

  def jTree1ValueChanged(evt: TreeSelectionEvent): Unit = {

    // preparo pannello

    jPanel2.removeAll()
    jPanel2.setSize(new Dimension(800, 800))

    execute.getActionListeners.foreach(execute.removeActionListener(_))

    //

    val selectedNode = jTree1.getLastSelectedPathComponent.asInstanceOf[DefaultMutableTreeNode]
    val descr = new Description

    descr.setTitle(selectedNode.toString)
    jPanel2.add(descr)

    gestisciMlQueries(selectedNode, descr)

    gestisciTagQueries(selectedNode, descr)

    gestisciPostQueries(selectedNode, descr)

    gestisciTrajQueries(selectedNode, descr)

    gestisciUsClQueries(selectedNode, descr)

    revalidate()
    repaint()
  }

  def gestisciMlQueries(selectedNode: DefaultMutableTreeNode, descr: Description): Unit = {

    if (selectedNode.equals(mlQueries_1)) {

      descr.setDescription(mlQueries_1_description)

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Seleziona immagine")
      combo.jComboBox1.setEditable(false)

      val images = new File("images").listFiles.filter(_.isFile).map(x => x.getName).sorted.toList

      for (image <- images)
        combo.jComboBox1.addItem(image)

      combo.jComboBox1.getEditor.setItem(images(0))

      jPanel2.add(combo)
      jPanel2.setSize(new Dimension(800, 800))

      execute.addActionListener((_: ActionEvent) => {

        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val imagePanel = new ImagePanel("images/" + combo.jComboBox1.getEditor.getItem.toString)

            val datasetchart = mlQuery.imageNet(combo.jComboBox1.getEditor.getItem.toString)(0)
              .map(x => (String.format("%4.2f%%", (x._1 * 100).asInstanceOf[Object]), x._2))
              .toDF("Probability", "Class")

            val table = new Table(datasetchart.columns, datasetchart.collect())

            table.scrollPane.setPreferredSize(new Dimension(500, 800))
            imagePanel.add(table)

            workerPanel.add(imagePanel)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(mlQueries_2)) {

      descr.setDescription(mlQueries_2_description)

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Inserisci id post")
      combo.jComboBox1.setEditable(true)

      for (post <- Array("6196171304", "14582346968", "16227723677"))
        combo.jComboBox1.addItem(post)

      jPanel2.add(combo)
      jPanel2.setSize(new Dimension(800, 800))

      execute.addActionListener((_: ActionEvent) => {

        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {
            val s: java.lang.String = combo.jComboBox1.getEditor.getItem.toString

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val ds = dataset.filter(x => x.id != null && x.id.equals(s)).dropDuplicates()

            val datasetchart = mlQuery.rowLanguage(ds)
              .map(x => (x.get("document"), x.get("language")))

            val post = ds.collect()(0)

            val label1 = new JTextArea("Titolo: \n" + post.title)
            label1.setSize(new Dimension(400, 50))
            label1.setEditable(false)
            label1.setLineWrap(true)
            label1.setBackground(Color.WHITE)

            val label2 = new JTextArea("Descrizione : \n" + post.description)
            label2.setPreferredSize(new Dimension(400, 400))
            label2.setEditable(false)
            label2.setLineWrap(true)

            val label3 = new JTextArea("Lingua Assegnata : " + datasetchart.collect()(0)._2.get.mkString(" "))
            label3.setSize(new Dimension(400, 50))
            label3.setEditable(false)
            label3.setLineWrap(true)
            label3.setBackground(Color.WHITE)

            workerPanel.add(label1)
            workerPanel.add(label2)
            workerPanel.add(label3)

            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)
          }
        }

        worker.execute()
      })

    }

  }

  def gestisciPostQueries(selectedNode: DefaultMutableTreeNode, descr: Description): Unit = {

    if (selectedNode.equals(postQueries_1_1)) {

      descr.setDescription(postQueries_1_1_description)

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val input = new IntInput(3, 10, 3, 1)
      input.jLabel1.setText("Limite top utenti")

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(input)
      jPanel2.add(date1Date2)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            val datasetchart = postQuery.mostInfluential_Post(ds)
              .limit(input.jSpinner1.getModel.getValue.asInstanceOf[Int])

            val barChart = BarChart.createPanel(datasetchart
              .map(x => (x.get(0).asInstanceOf[String], x.get(2).asInstanceOf[Double])).collect())

            val table = new Table(datasetchart.columns, datasetchart.collect())

            workerPanel.add(barChart)
            workerPanel.add(table)

            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(postQueries_1_2)) {

      descr.setDescription(postQueries_1_2_description)

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val input = new IntInput(3, 10, 3, 1)
      input.jLabel1.setText("Limite top utenti")

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(input)
      jPanel2.add(date1Date2)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            val datasetchart = postQuery.mostInfluential_Views(ds)
              .limit(input.jSpinner1.getModel.getValue.asInstanceOf[Int])

            val barChart = BarChart.createPanel(datasetchart
              .map(x => (x.get(0).asInstanceOf[String], x.get(2).asInstanceOf[Long].toDouble)).collect())

            val table = new Table(datasetchart.columns, datasetchart.collect())

            workerPanel.add(barChart)
            workerPanel.add(table)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(postQueries_1_3)) {

      descr.setDescription(postQueries_1_3_description)

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val input = new IntInput(3, 10, 3, 1)
      input.jLabel1.setText("Limite top utenti")

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(input)
      jPanel2.add(date1Date2)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            val datasetchart = postQuery.mostInfluential(ds)
              .limit(input.jSpinner1.getModel.getValue.asInstanceOf[Int])

            val barChart = BarChart.createPanel(datasetchart
              .map(x => (x.get(0).asInstanceOf[String], x.get(2).asInstanceOf[Double])).collect())


            val table = new Table(datasetchart.columns, datasetchart.collect())

            workerPanel.add(barChart)
            workerPanel.add(table)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(postQueries_2_1)) {

      descr.setDescription(postQueries_2_1_description)

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Inserisci id utente")
      combo.jComboBox1.setEditable(true)
      for (tag <- Array(
        "97629199@N00", "55391611@N00", "33399095@N00", "11432907@N00",
        "96291012@N00", "136373368@N02")
           ) {
        combo.jComboBox1.addItem(tag)
      }

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(date1Date2)
      jPanel2.add(combo)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            val datasetchart = postQuery.trendOfPostForUserByYear(ds,
              combo.jComboBox1.getEditor.getItem.toString)
              .map(x => (x.get(0).asInstanceOf[String], x.get(4).asInstanceOf[Long]))

            updateProgress("Creo il grafico", 80)

            val graph = TimeSeriesByYearPanel.createPanel(
              datasetchart.collect(),
              "user = " + combo.jComboBox1.getEditor.getItem.toString,
              "anno",
              "totale post"
            )

            graph.setSize(this.workerPanel.getSize)
            workerPanel.add(graph)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(postQueries_2_2)) {

      descr.setDescription(postQueries_2_2_description)

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Inserisci utente")
      combo.jComboBox1.setEditable(true)
      for (tag <- Array(
        "97629199@N00", "55391611@N00", "33399095@N00", "11432907@N00",
        "96291012@N00", "136373368@N02")
           ) {
        combo.jComboBox1.addItem(tag)
      }

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(date1Date2)
      jPanel2.add(combo)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            val datasetchart = postQuery.trendOfPostForUserByMonth(ds,
              combo.jComboBox1.getEditor.getItem.toString)
              .map(x => (x.get(0).asInstanceOf[String], x.get(4).asInstanceOf[Long]))


            updateProgress("Creo il grafico", 80)

            val graph = TimeSeriesByMonthPanel.createPanel(
              datasetchart.collect(),
              "user = " + combo.jComboBox1.getEditor.getItem.toString,
              "mese",
              "totale post"
            )

            graph.setSize(this.workerPanel.getSize)
            workerPanel.add(graph)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(postQueries_2_3)) {

      descr.setDescription(postQueries_2_3_description)

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Inserisci utente")
      combo.jComboBox1.setEditable(true)
      for (tag <- Array(
        "97629199@N00", "55391611@N00", "33399095@N00", "11432907@N00",
        "96291012@N00", "136373368@N02")
           ) {
        combo.jComboBox1.addItem(tag)
      }

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(date1Date2)
      jPanel2.add(combo)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            val datasetchart = postQuery.trendOfPostForUserByDay(ds,
              combo.jComboBox1.getEditor.getItem.toString)
              .map(x => (x.get(0).asInstanceOf[LocalDate], x.get(3).asInstanceOf[Long]))

            updateProgress("Creo il grafico", 80)

            val graph = TimeSeriesByDayPanel.createPanel(
              datasetchart.collect(),
              "user = " + combo.jComboBox1.getEditor.getItem.toString,
              "giorno",
              "totale post"
            )

            graph.setSize(this.workerPanel.getSize)
            workerPanel.add(graph)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(postQueries_2_4)) {

      descr.setDescription(postQueries_2_4_description)

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Inserisci id utente")
      combo.jComboBox1.setEditable(true)
      for (tag <- Array(
        "97629199@N00", "55391611@N00", "33399095@N00", "11432907@N00",
        "96291012@N00", "136373368@N02")
           ) {
        combo.jComboBox1.addItem(tag)
      }

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(date1Date2)
      jPanel2.add(combo)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            val datasetchart = postQuery.trendOfPostForUserByYear(ds,
              combo.jComboBox1.getEditor.getItem.toString)
              .map(x => (x.get(0).asInstanceOf[String], x.get(3).asInstanceOf[Long]))

            updateProgress("Creo il grafico", 80)

            val graph = TimeSeriesByYearPanel.createPanel(
              datasetchart.collect(),
              "user = " + combo.jComboBox1.getEditor.getItem.toString,
              "anno",
              "totale visualizzazioni"
            )

            graph.setSize(this.workerPanel.getSize)
            workerPanel.add(graph)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(postQueries_2_5)) {

      descr.setDescription(postQueries_2_5_description)

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Inserisci utente")
      combo.jComboBox1.setEditable(true)
      for (tag <- Array(
        "97629199@N00", "55391611@N00", "33399095@N00", "11432907@N00",
        "96291012@N00", "136373368@N02")
           ) {
        combo.jComboBox1.addItem(tag)
      }

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(date1Date2)
      jPanel2.add(combo)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            val datasetchart = postQuery.trendOfPostForUserByMonth(ds,
              combo.jComboBox1.getEditor.getItem.toString)
              .map(x => (x.get(0).asInstanceOf[String], x.get(3).asInstanceOf[Long]))


            updateProgress("Creo il grafico", 80)

            val graph = TimeSeriesByMonthPanel.createPanel(
              datasetchart.collect(),
              "user = " + combo.jComboBox1.getEditor.getItem.toString,
              "mese",
              "totale visualizzazioni"
            )

            graph.setSize(this.workerPanel.getSize)
            workerPanel.add(graph)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(postQueries_2_6)) {

      descr.setDescription(postQueries_2_6_description)

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Inserisci utente")
      combo.jComboBox1.setEditable(true)
      for (tag <- Array(
        "97629199@N00", "55391611@N00", "33399095@N00", "11432907@N00",
        "96291012@N00", "136373368@N02")
           ) {
        combo.jComboBox1.addItem(tag)
      }

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(date1Date2)
      jPanel2.add(combo)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            val datasetchart = postQuery.trendOfPostForUserByDay(ds,
              combo.jComboBox1.getEditor.getItem.toString)
              .map(x => (x.get(0).asInstanceOf[LocalDate], x.get(4).asInstanceOf[Long]))


            updateProgress("Creo il grafico", 80)

            val graph = TimeSeriesByDayPanel.createPanel(
              datasetchart.collect(),
              "user = " + combo.jComboBox1.getEditor.getItem.toString,
              "giorno",
              "totale visualizzazioni"
            )

            graph.setSize(this.workerPanel.getSize)
            workerPanel.add(graph)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(postQueries_3)) {

      descr.setDescription(postQueries_3_description)

      val input = new IntInput(5, 100, 10, 5)
      input.jLabel1.setText("Limite righe")

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Inserisci utente")
      combo.jComboBox1.setEditable(true)
      for (tag <- Array(
        "97629199@N00", "55391611@N00", "33399095@N00", "11432907@N00",
        "96291012@N00", "136373368@N02")
           ) {
        combo.jComboBox1.addItem(tag)
      }

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(input)
      jPanel2.add(date1Date2)
      jPanel2.add(combo)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            var datasetchart = postQuery.postOfUser(ds,
              combo.jComboBox1.getEditor.getItem.toString)
              .limit(input.jSpinner1.getModel.getValue.asInstanceOf[Int])

            val table = new Table(datasetchart.columns, datasetchart.collect())

            workerPanel.add(table)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

  }

  def gestisciTagQueries(selectedNode: DefaultMutableTreeNode, descr: Description): Unit = {

    if (selectedNode.equals(tagQueries_1)) {

      descr.setDescription(tagQueries_1_description)

      val input = new IntInput(5, 100, 10, 5)
      input.jLabel1.setText("Limite righe")

      val combo = new ComboInput
      combo.jLabel1.setText("Inserisci tag")
      combo.jComboBox1.setEditable(true)
      for (tag <- Array(
        "rome", "roma", "italy", "italia",
        "lazio", "nikon", "vatican",
        "rom", "street", "europe")
           ) {
        combo.jComboBox1.addItem(tag)
      }

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(input)
      jPanel2.add(date1Date2)
      jPanel2.add(combo)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            var datasetchart = tagQuery.postForTag(ds,
              combo.jComboBox1.getEditor.getItem.toString)
              .limit(input.jSpinner1.getModel.getValue.asInstanceOf[Int])

            datasetchart = datasetchart
              .select(monotonically_increasing_id.alias("nr"), col("idPost"), col("url"), col("tags"))

            val table = new Table(datasetchart.columns, datasetchart.collect())

            workerPanel.add(table)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(tagQueries_2_1)) {

      descr.setDescription(tagQueries_2_1_description)

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Inserisci tag")
      combo.jComboBox1.setEditable(true)
      for (tag <- Array(
        "rome", "roma", "italy", "italia",
        "lazio", "nikon", "vatican",
        "rom", "street", "europe")
           ) {
        combo.jComboBox1.addItem(tag)
      }

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(date1Date2)
      jPanel2.add(combo)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            val datasetchart = tagQuery.trendTagByYear(ds,
              combo.jComboBox1.getEditor.getItem.toString)
              .map(x => (x.get(0).asInstanceOf[String], x.get(1).asInstanceOf[Long]))

            updateProgress("Creo il grafico", 80)

            val graph = TimeSeriesByYearPanel.createPanel(
              datasetchart.collect(),
              "tag = " + combo.jComboBox1.getEditor.getItem.toString,
              "post per anno",
              "totale"
            )

            graph.setSize(this.workerPanel.getSize)
            workerPanel.add(graph)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(tagQueries_2_2)) {

      descr.setDescription(tagQueries_2_2_description)

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Inserisci tag")
      combo.jComboBox1.setEditable(true)
      for (tag <- Array(
        "rome", "roma", "italy", "italia",
        "lazio", "nikon", "vatican",
        "rom", "street", "europe")
           ) {
        combo.jComboBox1.addItem(tag)
      }

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(date1Date2)
      jPanel2.add(combo)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            val datasetchart = tagQuery.trendTagByMonth(ds,
              combo.jComboBox1.getEditor.getItem.toString)
              .map(x => (x.get(0).asInstanceOf[String], x.get(1).asInstanceOf[Long]))

            updateProgress("Creo il grafico", 80)

            val graph = TimeSeriesByMonthPanel.createPanel(
              datasetchart.collect(),
              "tag = " + combo.jComboBox1.getEditor.getItem.toString,
              "post per mese",
              "totale"
            )

            graph.setSize(this.workerPanel.getSize)
            workerPanel.add(graph)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(tagQueries_2_3)) {

      descr.setDescription(tagQueries_2_3_description)

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Inserisci tag")
      combo.jComboBox1.setEditable(true)
      for (tag <- Array(
        "rome", "roma", "italy", "italia",
        "lazio", "nikon", "vatican",
        "rom", "street", "europe")
           ) {
        combo.jComboBox1.addItem(tag)
      }

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(date1Date2)
      jPanel2.add(combo)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            val datasetchart = tagQuery.trendTagByDay(ds,
              combo.jComboBox1.getEditor.getItem.toString)
              .map(x => (x.get(0).asInstanceOf[LocalDate], x.get(2).asInstanceOf[Long]))

            updateProgress("Creo il grafico", 80)

            val graph = TimeSeriesByDayPanel.createPanel(
              datasetchart.collect(),
              "tag = " + combo.jComboBox1.getEditor.getItem.toString,
              "post per giorno",
              "totale"
            )

            graph.setSize(this.workerPanel.getSize)
            workerPanel.add(graph)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

    if (selectedNode.equals(tagQueries_3)) {

      descr.setDescription(tagQueries_3_description)

      val date1Date2: Date1Date2 = new Date1Date2
      date1Date2.setSize(new Dimension(800, 50))

      val date1 = date1Date2.jDateChooser1.getModel
      date1.addDay(-1)
      val date2 = date1Date2.jDateChooser2.getModel
      date2.addDay(+1)

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(date1Date2)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val year1 = date1.getYear.asInstanceOf[Integer]
            val month1 = date1.getMonth.asInstanceOf[Integer] + 1
            val day1 = date1.getDay.asInstanceOf[Integer]

            val year2 = date2.getYear.asInstanceOf[Integer]
            val month2 = date2.getMonth.asInstanceOf[Integer] + 1
            val day2 = date2.getDay.asInstanceOf[Integer]

            var ds = dataset
            if (date1.isSelected && date2.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC))
                  && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date1.isSelected)
              ds = dataset
                .filter(x => x.datePosted != null
                  && x.datePosted.isAfter(LocalDateTime.of(year1, month1, day1, 0, 0).toInstant(ZoneOffset.UTC)))

            else if (date2.isSelected)
            ds = dataset
              .filter(x => x.datePosted != null
                && x.datePosted.isBefore(LocalDateTime.of(year2, month2, day2, 0, 0).toInstant(ZoneOffset.UTC)))



            val datasetchart = tagQuery.mostUsedTags(ds)
              .map(x => (x.get(0).asInstanceOf[String], x.get(2).asInstanceOf[Int]))

            updateProgress("Creo il grafico", 80)

            val graph = PieChart.createPanel(
              datasetchart.collect()
            )

            graph.setSize(this.workerPanel.getSize)
            workerPanel.add(graph)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

  }

  def gestisciTrajQueries(selectedNode: DefaultMutableTreeNode, descr: Description): Unit = {

    if (selectedNode.equals(trajQueries_1)) {

      descr.setDescription(trajQueries_1_description)

      val input1 = new IntInput(5, 20, 5, 1)
      input1.jLabel1.setText("Limite percorsi per lunghezza")

      val input2 = new IntInput(2, 10, 2, 1)
      input2.jLabel1.setText("Lunghezza minima path")

      val input3 = new ComboInput
      input3.jLabel1.setText("Filtro nome luogo")
      input3.jComboBox1.setEditable(true)
      for (tag <- Array(
        "Colosseo", "Campidoglio", "Forum Romanum", "Basilica Sancti Petri",
        "Mausoleo di Adriano", "Piazza Venezia", "Musei Vaticani"
      )
           ) {
        input3.jComboBox1.addItem(tag)
      }

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(input1)
      jPanel2.add(input2)
      jPanel2.add(input3)

      execute.addActionListener((_: ActionEvent) => {

        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            var datasetchart = trajQuery.topPatterns(
              input1.jSpinner1.getModel.getValue.asInstanceOf[Int],
              input3.jComboBox1.getEditor.getItem.toString,
              input2.jSpinner1.getModel.getValue.asInstanceOf[Int]
            )

            datasetchart = datasetchart.map(x => (
              x(0).asInstanceOf[Int],
              "<html>" + x(1).asInstanceOf[Seq[Seq[String]]].map(t => t.mkString(" ")).mkString("<br> -> ") + "</html>",
              String.format("%4.2f%%", (x(2).asInstanceOf[Double]*100).asInstanceOf[Object]),
              x(3).asInstanceOf[Long])
            )
              .toDF("LENGTH", "PATTERN", "SUPPORT", "COUNT")


            val table = new Table(datasetchart.columns, datasetchart.collect())

            table.scrollPane.setPreferredSize(new Dimension(1200, 800))
            table.table.setRowHeight(30 * 10)
            table.table.getColumn("PATTERN").setMinWidth(600)

            workerPanel.add(table)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

  }

  def gestisciUsClQueries(selectedNode: DefaultMutableTreeNode, descr: Description): Unit = {

    if (selectedNode.equals(usClQueries_2_1)) {

      descr.setDescription(usClQueries_2_1_description)

      jPanel2.setSize(new Dimension(800, 800))

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {

          private def createChart(dataset: CategoryDataset) = {
            val chart = ChartFactory.createBarChart("", "cluster", "", dataset)

            chart.setBackgroundPaint(Color.WHITE)
            val plot = chart.getPlot.asInstanceOf[CategoryPlot]

            val rangeAxis = plot.getRangeAxis.asInstanceOf[NumberAxis]
            rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits)
            val renderer = plot.getRenderer.asInstanceOf[BarRenderer]
            renderer.setDrawBarOutline(false)
            chart.getLegend.setFrame(BlockBorder.NONE)
            chart
          }

          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val k = 6
            val dataset1 = new DefaultCategoryDataset
            val dataset2 = new DefaultCategoryDataset

            for (i <- (0 until k)) {
              val a = usClQuery.statistics_for_cluster(i, k, true)
              dataset1.addValue(a._1.toDouble, i.toString, "Num users")
              dataset2.addValue(a._2, i.toString, "Mean post per user")
              dataset1.addValue(a._3, i.toString, "Mean views per user")
              dataset2.addValue(a._4, i.toString, "Mean frequency (post per minute)")

            }

            val chart1 = createChart(dataset1)
            val chartPanel1 = new ChartPanel(chart1, false)
            chartPanel1.setFillZoomRectangle(true)
            chartPanel1.setMouseWheelEnabled(true)
            chartPanel1.setPreferredSize(new Dimension(1000, 400))

            val chart2 = createChart(dataset2)
            val chartPanel2 = new ChartPanel(chart2, false)
            chartPanel2.setFillZoomRectangle(true)
            chartPanel2.setMouseWheelEnabled(true)
            chartPanel2.setPreferredSize(new Dimension(1000, 400))


            updateProgress("Creo il grafico", 80)

            workerPanel.add(chartPanel1)
            workerPanel.add(chartPanel2)
            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }
        worker.execute()
      })

    }

    if (selectedNode.equals(usClQueries_2_2)) {

      descr.setDescription(usClQueries_2_2_description)

      val combo: ComboInput = new ComboInput
      combo.jLabel1.setText("Inserisci id utente")
      combo.jComboBox1.setEditable(true)
      for (tag <- Array(
        "97629199@N00", "55391611@N00", "33399095@N00", "11432907@N00",
        "96291012@N00", "136373368@N02")
           ) {
        combo.jComboBox1.addItem(tag)
      }

      val input1 = new IntInput(5, 20, 5, 1)
      input1.jLabel1.setText("Limite utenti")

      jPanel2.setSize(new Dimension(800, 800))

      jPanel2.add(combo)
      jPanel2.add(input1)

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            val ds = usClQuery.load_user_embeddings(true)
            val userId: String = combo.jComboBox1.getEditor.getItem.toString

            val model = usClQuery.userClustering(6, ds.toDF())
            val clusterAssignments = model.transform(ds).toDF("USER", "EMBEDDING", "CLUSTER")

            val ds_not_user = clusterAssignments
              .filter(x => !x.get(x.fieldIndex("USER")).toString.equals(userId))

            val ds_user = clusterAssignments
              .filter(x => x.get(x.fieldIndex("USER")).toString.equals(userId)).collect()

            val userDescr = new Description

            userDescr.setTitle(combo.jComboBox1.getEditor.getItem.toString)
            userDescr.setDescription("L'utente appartiene al cluster " + ds_user(0).get(ds_user(0).fieldIndex("CLUSTER")))

            if (ds_user.length == 0) {
              workerPanel.add(new JLabel("Utente non trovato!"))
            } else {

              val datasetchart = ds_not_user
                .map(x =>
                  (
                    x.get(x.fieldIndex("USER")).toString,
                    Vectors
                      .sqdist
                      (
                        Vectors.dense(x.get(x.fieldIndex("EMBEDDING")).asInstanceOf[Seq[Double]].toArray),
                        Vectors.dense(ds_user(0).get(ds_user(0).fieldIndex("EMBEDDING")).asInstanceOf[Seq[Double]].toArray)
                      ),
                    x.get(x.fieldIndex("CLUSTER")).asInstanceOf[Int]
                  )
                )
                .toDF("USER", "DIST", "CLUSTER")
                .sort("DIST")
                .limit(input1.jSpinner1.getModel.getValue.asInstanceOf[Int])

              val table = new Table(datasetchart.columns, datasetchart.collect())

              workerPanel.add(userDescr)
              workerPanel.add(table)
              updateProgress("Creo il grafico", 80)

            }

            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }
        worker.execute()
      })

    }

    if (selectedNode.equals(usClQueries_2_3)) {

      descr.setDescription(usClQueries_2_3_description)

      val input1 = new IntInput(0, 5, 0, 1)
      input1.jLabel1.setText("Cluster")

      jPanel2.add(input1)

      jPanel2.setSize(new Dimension(800, 800))

      execute.addActionListener((_: ActionEvent) => {
        val worker: AbstractWorker = new AbstractWorker(self) {
          override def doInBackground(): Unit = {

            updateProgress("Carico il database", 0)
            updateProgress("Eseguo la query", 30)

            println(input1.jSpinner1.getModel.getValue.asInstanceOf[Int])

            val a: Int = input1.jSpinner1.getModel.getValue.asInstanceOf[Int]


            val datasetchart = usClQuery.representatives(5, true, 6)
              .filter(_ (0).asInstanceOf[Int] == a)

            val datasetchart1 = datasetchart.collect().map(
              x =>
                (
                  x(1).toString,
                  postQuery
                    .mostUsedWords(dataset.filter(y => y.owner.id.equals(x(1).toString))).limit(10)
                    .map(z => z(0).toString).collect().mkString(", ")
                )
            ).toSeq.toDF("USER", "MOST_USED_WORDS")

            val datasetchart2 = datasetchart.collect().map(
              x =>
                (
                  x(1).toString,
                  tagQuery
                    .mostUsedTags(dataset.filter(y => y.owner.id.equals(x(1).toString))).limit(10)
                    .map(z => z(0).toString).collect().mkString(", ")
                )
            ).toSeq.toDF("USER", "MOST_USED_TAGS")

            val datasetchart3 = datasetchart1.join(datasetchart2, "USER")

            val table = new Table(datasetchart3.columns, datasetchart3.collect())

            workerPanel.add(table)

            workerFrame.add(workerPanel)
            updateProgress("Task completato", 100)

          }
        }

        worker.execute()
      })

    }

  }

  def updateProgress(str: String, i: Int): Unit = {
    jTextArea1.setText(String.format("%30s", new Date()) + " : " + str)
    jProgressBar1.setValue(i)
  }

  def setGlobalFont(font: Font): Unit = {
    val keys = UIManager.getLookAndFeelDefaults.keys()
    while (keys.hasMoreElements) {
      val key = keys.nextElement()
      val value = UIManager.get(key)
      if (value.isInstanceOf[Font]) {
        UIManager.put(key, font)
      }
    }
  }

}
