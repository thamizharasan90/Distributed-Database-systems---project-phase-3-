package io.purush.spark.giscup.java.solution

/**
  * Created by pswaminathan on 12/22/16.
  */
object ScalaHelper {
  def findNeighbor(x: Int, t: Int): List[(Int, Int)] = {
    /*Ugliest scala function ever written*/

    val ycc = ycells + 1
    var result = List[(Int, Int)]()

    if (x == 0) {
      // Left Bottom
      List(
        (x, t - 1),
        (x, t + 1),

        (x + 1, t - 1),
        (x + 1, t),
        (x + 1, t + 1),

        (x + ycc + 1, t - 1),
        (x + ycc + 1, t),
        (x + ycc + 1, t + 1),

        (x + ycc, t - 1),
        (x + ycc, t),
        (x + ycc, t + 1)

      )
    } else if (x == (xcells * ycc + ycells)) {
      //top right
      List(
        (x, t - 1),
        (x, t + 1),

        (x - 1, t - 1),
        (x - 1, t),
        (x - 1, t + 1),

        (x - ycc, t - 1),
        (x - ycc, t),
        (x - ycc, t + 1),

        (x - ycc - 1, t - 1),
        (x - ycc - 1, t),
        (x - ycc - 1, t + 1)

      )
    } else if (x == (ycells)) {
      //left top
      List(
        (x, t - 1),
        (x, t + 1),

        (x - 1, t - 1),
        (x - 1, t),
        (x - 1, t + 1),

        (x + ycc, t - 1),
        (x + ycc, t),
        (x + ycc, t + 1),

        (x + ycc - 1, t - 1),
        (x + ycc - 1, t),
        (x + ycc - 1, t + 1)

      )
    } else if (x == (xcells * ycc)) {
      //right bottom
      List(
        (x, t - 1),
        (x, t + 1),

        (x + 1, t - 1),
        (x + 1, t),
        (x + 1, t + 1),

        (x - ycc, t - 1),
        (x - ycc, t),
        (x - ycc, t + 1),

        (x - ycc + 1, t - 1),
        (x - ycc + 1, t),
        (x - ycc + 1, t + 1)

      )
    } else if (x % ycc == 0) {
      //Bottom gutter
      List(
        (x, t - 1),
        (x, t + 1),

        (x + 1, t - 1),
        (x + 1, t),
        (x + 1, t + 1),

        (x + ycc, t - 1),
        (x + ycc, t),
        (x + ycc, t + 1),

        (x + ycc + 1, t - 1),
        (x + ycc + 1, t),
        (x + ycc + 1, t + 1),

        (x - ycc, t - 1),
        (x - ycc, t),
        (x - ycc, t + 1),

        (x - ycc + 1, t - 1),
        (x - ycc + 1, t),
        (x - ycc + 1, t + 1)
      )
    } else if ((1 until xcells).contains(x)) {
      //Left gutter
      List(
        (x, t - 1),
        (x, t + 1),

        (x + 1, t - 1),
        (x + 1, t),
        (x + 1, t + 1),

        (x - 1, t - 1),
        (x - 1, t),
        (x - 1, t + 1),

        (x + ycc, t - 1),
        (x + ycc, t),
        (x + ycc, t + 1),

        (x + ycc + 1, t - 1),
        (x + ycc + 1, t),
        (x + ycc + 1, t + 1),

        (x + ycc - 1, t - 1),
        (x + ycc - 1, t),
        (x + ycc - 1, t + 1)

      )
    } else if ((xcells * ycc to xcells * ycc + ycells).contains(x)) {
      //Right gutter
      List(
        (x, t - 1),
        (x, t + 1),

        (x + 1, t - 1),
        (x + 1, t),
        (x + 1, t + 1),

        (x - 1, t - 1),
        (x - 1, t),
        (x - 1, t + 1),

        (x - ycc, t - 1),
        (x - ycc, t),
        (x - ycc, t + 1),

        (x - ycc + 1, t - 1),
        (x - ycc + 1, t),
        (x - ycc + 1, t + 1),

        (x - ycc - 1, t - 1),
        (x - ycc - 1, t),
        (x - ycc - 1, t + 1)

      )
    } else if ((x + 1) % ycc == 0) {
      //top gutter
      List(
        (x, t - 1),
        (x, t + 1),

        (x - 1, t - 1),
        (x - 1, t),
        (x - 1, t + 1),

        (x - ycc, t - 1),
        (x - ycc, t),
        (x - ycc, t + 1),

        (x - ycc - 1, t - 1),
        (x - ycc - 1, t),
        (x - ycc - 1, t + 1),

        (x + ycc, t - 1),
        (x + ycc, t),
        (x + ycc, t + 1),

        (x + ycc - 1, t - 1),
        (x + ycc - 1, t),
        (x + ycc - 1, t + 1))
    }
    else {

      List((x + 1, t), (x - 1, t),
        (x + ycc - 1, t), (x + ycc, t), (x + ycc + 1, t),
        (x - ycc - 1, t), (x - ycc, t), (x - ycc + 1, t),
        (x + 1, t - 1), (x - 1, t - 1), (x, t - 1),
        (x + ycc - 1, t - 1), (x + ycc, t - 1), (x + ycc + 1, t - 1),
        (x - ycc - 1, t - 1), (x - ycc, t - 1), (x - ycc + 1, t - 1),
        (x + 1, t + 1), (x - 1, t + 1), (x, t + 1),
        (x + ycc - 1, t + 1), (x + ycc, t + 1), (x + ycc + 1, t + 1),
        (x - ycc - 1, t + 1), (x - ycc, t + 1), (x - ycc + 1, t + 1)
      )
    }
  }

  def getis(xt: (Int, Int)): Double = {
    val neighs = findNeighbor(xt._1, xt._2).filter(checkTimeBounds) :+ (xt._1, xt._2)
    val W = neighs.size
    val denominator = S * math.sqrt((n * W - W * W) / (n - 1))
    val neighVals = neighs.map(y => cellValuesMap.get(y))
    val nrSecondHalf = X * W
    val nrFirstHalf = neighVals.foldLeft(0)((a, x) => a + (if (x.isEmpty) 0 else x.get))
    val numerator = nrFirstHalf - nrSecondHalf
    numerator / denominator
  }
}
