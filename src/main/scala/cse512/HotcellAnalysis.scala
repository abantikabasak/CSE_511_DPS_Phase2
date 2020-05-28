package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.createOrReplaceTempView("pickupinfo")
  pickupInfo.show()



  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // Register the user-defined functions
		spark.udf.register("count_of_neighbors", (X: Int, Y: Int, Z: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int) => ((HotcellUtils.count_of_neighbors(X, Y, Z, minX, minY, minZ, maxX, maxY, maxZ))))

		 spark.udf.register("z_score", (neighbors_value_sum: Int, mean: Double, num_of_neighbors: Int, std_dev: Double, num_of_cells: Int) => ((HotcellUtils.z_score(neighbors_value_sum, mean, num_of_neighbors, std_dev, num_of_cells))))

		 spark.udf.register("square", (v: Int) => HotcellUtils.square(v))

   	val pointsInRangeXYZ = spark.sql("SELECT x, y, z, count(*) as attributes FROM pickupinfo WHERE x >= " + minX + " AND y >= " + minY  + " AND z >= " + minZ + " AND x <= " + maxX + " AND y <= " + maxY +  " AND z <= " + maxZ + " GROUP BY x,y,z")
    pointsInRangeXYZ.createOrReplaceTempView("pointsInRangeXYZ")
    //pointsInRangeXYZ.show()

		val sumAttributes = spark.sql("SELECT SUM(attributes) AS attributeSum, SUM(square(attributes)) AS attributeSquaredSum FROM pointsInRangeXYZ")
    sumAttributes.createOrReplaceTempView("sumAttributes")
    //sumofPoints.show()

    val mean = sumAttributes.first().getLong(0).toDouble / numCells.toDouble
    val stdev = math.sqrt((sumAttributes.first().getDouble(1) / numCells.toDouble) - (mean * mean))

  	val neighbors = spark.sql("SELECT COUNT(*) AS neighborsCount, P1.x as x, P1.y as y, P1.z as z, SUM(P2.attributes) AS neighborsAttributeCount FROM pointsInRangeXYZ AS P1, pointsInRangeXYZ AS P2 WHERE (P2.x = P1.x + 1 or P2.x = P1.x or P2.x = P1.x - 1) AND (P2.y = P1.y + 1 or P2.y = P1.y or P2.y = P1.y - 1) AND (P2.z = P1.z + 1 or P2.z = P1.z OR P2.z = P1.z - 1) GROUP BY P1.x, P1.y, P1.z")
	neighbors.createOrReplaceTempView("neighbors")
	//neighbors.show()
    
  val zScore = spark.sql("SELECT x,y,z ,z_Score(neighborsAttributeCount, "+ mean + ", neighborsCount, " + stdev + ", " + numCells + ") as zscore, neighborsAttributeCount, neighborsCount from neighbors order by zscore desc limit 50");
  zScore.createOrReplaceTempView("zScore")
  zScore.show()

	val count = spark.sql("select count(*),neighborsCount from neighbors group by neighborsCount");
	count.createOrReplaceTempView("count")
	count.show()

  val finalResult = spark.sql("select x, y, z from zScore")
  finalResult.createOrReplaceTempView("finalResult")
  finalResult.show()

  return finalResult // YOU NEED TO CHANGE THIS PART
}
}

