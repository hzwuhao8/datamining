package guidetodatamining.ch3

import guidetodatamining.ch2.Recommender.UserMap

trait MovieData {
  val path = "data/ml-100k/"
  val ratingfile = path + "u.data"
  val userfile = path + "u.user"
  val moviefile = path + "u.item"
  
  
  def loadR( ratingfile: String = ratingfile): UserMap = {
    val lines = scala.io.Source.fromFile(ratingfile).getLines()
    val seq = lines.map { line =>
      val fields = line.split("\t")
      val Array(uid, itemid, rating, _) = fields
      (uid -> (itemid -> rating.toDouble))

    }

    val z0 = Map[String, Map[String, Double]]()
    seq.foldLeft(z0) { (a, b) =>
      a.get(b._1) match {
        case None => a ++ Map(b._1 -> Map(b._2._1 -> b._2._2)) // 还没有任何评分
        case Some(d) => d.get(b._2._1) match {
          case None     => a ++ Map(b._1 -> d.+(b._2._1 -> b._2._2)) // 已经对其他书进行了评分
          case Some(dd) => a ++ Map(b._1 -> d.+(b._2._1 -> (dd + b._2._2))) //重复评分，好像是不可能的
        }
      }
    }

  }

  def loadM(): Map[String, String] = {
    val lines = org.apache.commons.io.FileUtils.readLines(new java.io.File(moviefile))

    val seq = scala.collection.convert.WrapAsScala.asScalaBuffer(lines).flatMap { line =>
      try {
        
        val fields = java.net.URLDecoder.decode(line).split("\\|")
        
        val res = if (fields.size >= 2) {
          val mid = fields(0)
          val name = fields(1)
          Some((mid -> name))
        } else {
          None
        }

        res
      } catch {
        case _: Exception => None
      }
    }
    seq.toMap
  }
  
}