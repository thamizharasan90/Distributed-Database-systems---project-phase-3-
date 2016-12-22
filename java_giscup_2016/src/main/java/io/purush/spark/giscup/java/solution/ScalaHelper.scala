package io.purush.spark.giscup.java.solution

/**
  * Created by pswaminathan on 12/22/16.
  */
object ScalaHelper {

  def findNeighbor(x: Integer, t: Integer, xcells:Integer, ycells:Integer): List[(Integer, Integer)] = {
    /*Ugliest scala function ever written*/

    val ycc = ycells + 1
    var result = List[(Integer, Integer)]()

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
}
