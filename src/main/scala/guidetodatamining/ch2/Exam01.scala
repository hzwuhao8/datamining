package guidetodatamining.ch2

object Exam01 extends util.Log {
  import Recommender._

  val users: UserMap = Map("Angelica" -> Map("Blues Traveler" -> 3.5, "Broken Bells" -> 2.0, "Norah Jones" -> 4.5, "Phoenix" -> 5.0, "Slightly Stoopid" -> 1.5, "The Strokes" -> 2.5, "Vampire Weekend" -> 2.0),
    "Bill" -> Map("Blues Traveler" -> 2.0, "Broken Bells" -> 3.5, "Deadmau5" -> 4.0, "Phoenix" -> 2.0, "Slightly Stoopid" -> 3.5, "Vampire Weekend" -> 3.0),
    "Chan" -> Map("Blues Traveler" -> 5.0, "Broken Bells" -> 1.0, "Deadmau5" -> 1.0, "Norah Jones" -> 3.0, "Phoenix" -> 5, "Slightly Stoopid" -> 1.0),
    "Dan" -> Map("Blues Traveler" -> 3.0, "Broken Bells" -> 4.0, "Deadmau5" -> 4.5, "Phoenix" -> 3.0, "Slightly Stoopid" -> 4.5, "The Strokes" -> 4.0, "Vampire Weekend" -> 2.0),
    "Hailey" -> Map("Broken Bells" -> 4.0, "Deadmau5" -> 1.0, "Norah Jones" -> 4.0, "The Strokes" -> 4.0, "Vampire Weekend" -> 1.0),
    "Jordyn" -> Map("Broken Bells" -> 4.5, "Deadmau5" -> 4.0, "Norah Jones" -> 5.0, "Phoenix" -> 5.0, "Slightly Stoopid" -> 4.5, "The Strokes" -> 4.0, "Vampire Weekend" -> 4.0),
    "Sam" -> Map("Blues Traveler" -> 5.0, "Broken Bells" -> 2.0, "Norah Jones" -> 3.0, "Phoenix" -> 5.0, "Slightly Stoopid" -> 4.0, "The Strokes" -> 5.0),
    "Veronica" -> Map("Blues Traveler" -> 3.0, "Norah Jones" -> 5.0, "Phoenix" -> 4.0, "Slightly Stoopid" -> 2.5, "The Strokes" -> 3.0))

  def main(args: Array[String]): Unit = {
    val uh = "Hailey"
    val ua = "Angelica"
    val ub = "Bill"
    val uj = "Jordyn"
    val List(u1, u2) = List(uh, "Veronica")
    log.info("距离 = {}", manhattan(users(u1), users(u2)))

    log.info("距离 = {}", manhattan(users(uh), users(uj)))
    computeNearestNeighbor(uh, users, manhattan)
    recommedManhattan(uh, users)

    recommedManhattan("Chan", users)

    recommedManhattan("Sam", users)
    recommedManhattan("Angelica", users)

    log.info(s"pearson ${ua},${ub} = {} ", pearson(users(ua), users(ub)))
    log.info(s"pearson ${ua},${uh} = {} ", pearson(users(ua), users(uh)))
    log.info(s"pearson ${ua},${uj} = {} ", pearson(users(ua), users(uj)))
    recommedManhattan(uh, users)
    recommedPearson(uh, users)
    recommed(uh,users, pearsonMetric,2)
    recommed(uj,users, pearsonMetric,1)
  }
}