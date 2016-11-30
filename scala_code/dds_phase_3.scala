
object Cells {
  val textFile = "hdfs://localhost:54310/input"

  /* ... new cell ... */

  val csvFileRDD = sc textFile textFile
  csvFileRDD.count

  /* ... new cell ... */

  val first = csvFileRDD.first
  val filteredCSVRDD = csvFileRDD filter {x => x!=first }
  filteredCSVRDD.take(1)

  /* ... new cell ... */

  def roundUp(d:String): Double = BigDecimal(d.toDouble).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  def splitFilter(s:Array[String]): (Int, Double, Double) = {
    
    (s(1).split(" ")(0).split("-")(1).toInt, roundUp(s(5)), roundUp(s(6)))
  }

  /* ... new cell ... */

  val filteredBeforeCelling = filteredCSVRDD.map( x => (splitFilter(x.split(","))))

  /* ... new cell ... */

  val orderByLongitude = new Ordering[(Int,Double,Double)]() {
    override def compare(x: (Int, Double, Double), y: (Int,Double,Double)): Int = 
      Ordering[Double].compare(x._2,y._2)
  }
  val maxLongitude = filteredBeforeCelling.max()(orderByLongitude)
  val minLongitude = filteredBeforeCelling.min()(orderByLongitude)

  /* ... new cell ... */

  filteredBeforeCelling.take(2)

  /* ... new cell ... */
}
                  