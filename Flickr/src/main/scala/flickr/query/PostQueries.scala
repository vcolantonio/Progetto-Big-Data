package flickr.query

import flickr.beans.FlickrPost
import flickr.utils.Utils._
import org.apache.spark.sql.functions.{col, desc, to_date}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class PostQueries(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val stop_words_it = Array(" ", "a", "abbastanza", "abbia", "abbiamo", "abbiano", "abbiate", "accidenti", "ad", "adesso", "affinché", "agl", "agli", "ahime", "ahimè", "ai", "al", "alcuna", "alcuni", "alcuno", "all", "alla", "alle", "allo", "allora", "altre", "altri", "altrimenti", "altro", "altrove", "altrui", "anche", "ancora", "anni", "anno", "ansa", "anticipo", "assai", "attesa", "attraverso", "avanti", "avemmo", "avendo", "avente", "aver", "avere", "averlo", "avesse", "avessero", "avessi", "avessimo", "aveste", "avesti", "avete", "aveva", "avevamo", "avevano", "avevate", "avevi", "avevo", "avrai", "avranno", "avrebbe", "avrebbero", "avrei", "avremmo", "avremo", "avreste", "avresti", "avrete", "avrà", "avrò", "avuta", "avute", "avuti", "avuto", "basta", "ben", "bene", "benissimo", "brava", "bravo", "buono", "c", "caso", "cento", "certa", "certe", "certi", "certo", "che", "chi", "chicchessia", "chiunque", "ci", "ciascuna", "ciascuno", "cima", "cinque", "cio", "cioe", "cioè", "circa", "citta", "città", "ciò", "co", "codesta", "codesti", "codesto", "cogli", "coi", "col", "colei", "coll", "coloro", "colui", "come", "cominci", "comprare", "comunque", "con", "concernente", "conclusione", "consecutivi", "consecutivo", "consiglio", "contro", "cortesia", "cos", "cosa", "cosi", "così", "cui", "d", "da", "dagl", "dagli", "dai", "dal", "dall", "dalla", "dalle", "dallo", "dappertutto", "davanti", "degl", "degli", "dei", "del", "dell", "della", "delle", "dello", "dentro", "detto", "deve", "devo", "di", "dice", "dietro", "dire", "dirimpetto", "diventa", "diventare", "diventato", "dopo", "doppio", "dov", "dove", "dovra", "dovrà", "dovunque", "due", "dunque", "durante", "e", "ebbe", "ebbero", "ebbi", "ecc", "ecco", "ed", "effettivamente", "egli", "ella", "entrambi", "eppure", "era", "erano", "eravamo", "eravate", "eri", "ero", "esempio", "esse", "essendo", "esser", "essere", "essi", "ex", "fa", "faccia", "facciamo", "facciano", "facciate", "faccio", "facemmo", "facendo", "facesse", "facessero", "facessi", "facessimo", "faceste", "facesti", "faceva", "facevamo", "facevano", "facevate", "facevi", "facevo", "fai", "fanno", "farai", "faranno", "fare", "farebbe", "farebbero", "farei", "faremmo", "faremo", "fareste", "faresti", "farete", "farà", "farò", "fatto", "favore", "fece", "fecero", "feci", "fin", "finalmente", "finche", "fine", "fino", "forse", "forza", "fosse", "fossero", "fossi", "fossimo", "foste", "fosti", "fra", "frattempo", "fu", "fui", "fummo", "fuori", "furono", "futuro", "generale", "gente", "gia", "giacche", "giorni", "giorno", "giu", "già", "gli", "gliela", "gliele", "glieli", "glielo", "gliene", "grande", "grazie", "gruppo", "ha", "haha", "hai", "hanno", "ho", "i", "ie", "ieri", "il", "improvviso", "in", "inc", "indietro", "infatti", "inoltre", "insieme", "intanto", "intorno", "invece", "io", "l", "la", "lasciato", "lato", "le", "lei", "li", "lo", "lontano", "loro", "lui", "lungo", "luogo", "là", "ma", "macche", "magari", "maggior", "mai", "male", "malgrado", "malissimo", "me", "medesimo", "mediante", "meglio", "meno", "mentre", "mesi", "mezzo", "mi", "mia", "mie", "miei", "mila", "miliardi", "milioni", "minimi", "mio", "modo", "molta", "molti", "moltissimo", "molto", "momento", "mondo", "ne", "negl", "negli", "nei", "nel", "nell", "nella", "nelle", "nello", "nemmeno", "neppure", "nessun", "nessuna", "nessuno", "niente", "no", "noi", "nome", "non", "nondimeno", "nonostante", "nonsia", "nostra", "nostre", "nostri", "nostro", "novanta", "nove", "nulla", "nuovi", "nuovo", "o", "od", "oggi", "ogni", "ognuna", "ognuno", "oltre", "oppure", "ora", "ore", "osi", "ossia", "ottanta", "otto", "paese", "parecchi", "parecchie", "parecchio", "parte", "partendo", "peccato", "peggio", "per", "perche", "perchè", "perché", "percio", "perciò", "perfino", "pero", "persino", "persone", "però", "piedi", "pieno", "piglia", "piu", "piuttosto", "più", "po", "pochissimo", "poco", "poi", "poiche", "possa", "possedere", "posteriore", "posto", "potrebbe", "preferibilmente", "presa", "press", "prima", "primo", "principalmente", "probabilmente", "promesso", "proprio", "puo", "pure", "purtroppo", "può", "qua", "qualche", "qualcosa", "qualcuna", "qualcuno", "quale", "quali", "qualunque", "quando", "quanta", "quante", "quanti", "quanto", "quantunque", "quarto", "quasi", "quattro", "quel", "quella", "quelle", "quelli", "quello", "quest", "questa", "queste", "questi", "questo", "qui", "quindi", "quinto", "realmente", "recente", "recentemente", "registrazione", "relativo", "riecco", "rispetto", "salvo", "sara", "sarai", "saranno", "sarebbe", "sarebbero", "sarei", "saremmo", "saremo", "sareste", "saresti", "sarete", "sarà", "sarò", "scola", "scopo", "scorso", "se", "secondo", "seguente", "seguito", "sei", "sembra", "sembrare", "sembrato", "sembrava", "sembri", "sempre", "senza", "sette", "si", "sia", "siamo", "siano", "siate", "siete", "sig", "solito", "solo", "soltanto", "sono", "sopra", "soprattutto", "sotto", "spesso", "sta", "stai", "stando", "stanno", "starai", "staranno", "starebbe", "starebbero", "starei", "staremmo", "staremo", "stareste", "staresti", "starete", "starà", "starò", "stata", "state", "stati", "stato", "stava", "stavamo", "stavano", "stavate", "stavi", "stavo", "stemmo", "stessa", "stesse", "stessero", "stessi", "stessimo", "stesso", "steste", "stesti", "stette", "stettero", "stetti", "stia", "stiamo", "stiano", "stiate", "sto", "su", "sua", "subito", "successivamente", "successivo", "sue", "sugl", "sugli", "sui", "sul", "sull", "sulla", "sulle", "sullo", "suo", "suoi", "tale", "tali", "talvolta", "tanto", "te", "tempo", "terzo", "th", "ti", "titolo", "tra", "tranne", "tre", "trenta", "triplo", "troppo", "trovato", "tu", "tua", "tue", "tuo", "tuoi", "tutta", "tuttavia", "tutte", "tutti", "tutto", "uguali", "ulteriore", "ultimo", "un", "una", "uno", "uomo", "va", "vai", "vale", "vari", "varia", "varie", "vario", "verso", "vi", "vicino", "visto", "vita", "voi", "volta", "volte", "vostra", "vostre", "vostri", "vostro", "è")
  val stop_words_en = Array("i", "the", "a", "me", "my", "myself", "we", "ourih", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now")

  def mostUsedWords(dataset: Dataset[FlickrPost]): Dataset[Row] = {
    var words = dataset
      .map(x => get_text(x.title, x.description))
      .flatMap(x => x.split("[^A-Za-z0-9]+"))
      .filter(x => !stop_words_it.contains(x) && !stop_words_en.contains(x))
      .map(x => (x.toLowerCase, 1))
      .groupByKey(x => x._1)
      .reduceGroups((x, y) => (x._1, x._2 + y._2))
      .map(x => (x._1, x._2._2))
      .toDF("word", "count")
      .sort(desc("count"))

    return words

  }

  private def get_text(title: String, description: String): String = {
    if (title == null && description == null)
      ""
    else if (title == null)
      description
    else if (description == null)
      title
    else
      title + " " + description
  }

  def mostInfluential(dataset: Dataset[FlickrPost]): Dataset[Row] = {
    var a = dataset
      .map(x => (x.owner.id, x.owner.username, x.views, 1d))
      .groupByKey(x => x._1)
      .mapValues(x => (x._2, x._3, x._4))
      .reduceGroups((x, y) => (x._1, x._2 + y._2, x._3 + y._3))
      .map(x => (x._1, x._2._1, influenceScore(x._2._2, x._2._3)))
      .toDF("userid", "username", "inflScore")
      .sort(desc("inflScore"))

    return a
  }

  private def influenceScore(views: Long, count: Double): Double = views / count + Math.log10(count)

  def mostInfluential_Views(dataset: Dataset[FlickrPost]): Dataset[Row] = {
    var a = dataset
      .map(x => (x.owner.id, x.owner.username, x.views, 1d))
      .groupByKey(x => x._1)
      .mapValues(x => (x._2, x._3, x._4))
      .reduceGroups((x, y) => (x._1, x._2 + y._2, x._3 + y._3))
      .map(x => (x._1, x._2._1, x._2._2))
      .toDF("userid", "username", "inflScore_Views")
      .sort(desc("inflScore_Views"))

    return a
  }

  def mostInfluential_Post(dataset: Dataset[FlickrPost]): Dataset[Row] = {
    var a = dataset
      .map(x => (x.owner.id, x.owner.username, x.views, 1d))
      .groupByKey(x => x._1)
      .mapValues(x => (x._2, x._3, x._4))
      .reduceGroups((x, y) => (x._1, x._2 + y._2, x._3 + y._3))
      .map(x => (x._1, x._2._1, x._2._3))
      .toDF("userid", "username", "inflScore_Posts")
      .sort(desc("inflScore_Posts"))

    return a
  }

  def trendOfPostForUserByYear(dataset: Dataset[FlickrPost], user: String): Dataset[Row] = {
    var a = trendOfPostForUserByDay(dataset, user)
      .withColumn("year", to_date_en_year(col("date")))
      .drop("date")
    var b = a.groupBy("year").agg(Map("total views" -> "sum"))
    var c = a.groupBy("year")
      .count()
      .toDF("year", "total post")
    var ris = a.join(b, "year")
      .join(c, "year").drop("number of post")
      .drop("total views")
      .dropDuplicates("year")

    return ris
  }

  def trendOfPostForUserByDay(dataset: Dataset[FlickrPost], user: String): Dataset[Row] = {
    var a = postOfUser(dataset, user)
    var b = a.join(postOfUserForDate(a), "date")
      .join(viewsOfUserForDate(a), "date")
      .drop("idPost", "views")
      .dropDuplicates("date")

    return b
  }

  def postOfUser(dataset: Dataset[FlickrPost], user: String): Dataset[Row] = {
    var a = dataset.filter(_.owner.id.equals(user)).map(x => (x.owner.id, x.id, x.datePosted, x.views, x.url))
      .toDF("idUtente", "idPost", "datePosted", "views", "url")
    //var dates_1 = res3.withColumn("DATE", to_date(col("TIMESTAMP"), "yyyy-MM-dd"))
    var b = a.withColumn("date", to_date(col("datePosted"), "yyyy-MM-dd"))
      .drop("datePosted")
    return b
  }

  private def postOfUserForDate(dataset: Dataset[Row]): Dataset[Row] = {
    dataset
      .groupBy("date")
      .count()
      .toDF("date", "number of post")
  }

  private def viewsOfUserForDate(dataset: Dataset[Row]): Dataset[Row] = {
    dataset
      .groupBy("date").agg(Map("views" -> "sum"))
      .withColumnRenamed("sum(views)", "total views")
  }

  def trendOfPostForUserByMonth(dataset: Dataset[FlickrPost], user: String): Dataset[Row] = {
    var a = trendOfPostForUserByDay(dataset, user)
      .withColumn("month", to_date_en_month(col("date")))
      .drop("date")
    var b = a.groupBy("month").agg(Map("total views" -> "sum"))
    var c = a.groupBy("month")
      .count()
      .toDF("month", "total post")
    var ris = a.join(b, "month").drop()
      .join(c, "month").drop("number of post")
      .drop("total views")
      .dropDuplicates("month")

    return ris
  }

  private def influenceScoreLog(views: Long, count: Double): Double = Math.log(count + 1) * views

}


