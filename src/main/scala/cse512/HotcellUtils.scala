package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def square (v: Int): Double =
  {
   	return v*v
  }

  def count_of_neighbors(X: Int, Y: Int, Z:Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int): Int = 
  {	
    var edgeX = 0
    var edgeY = 0
    var edgeZ = 0

		if(X == minX || X == maxX) 
			edgeX = 1
		else 
			edgeX = 0

		if(Y == minY || Y == maxY) 
			edgeY = 1
		else 
			edgeY = 0

		if(Z == minZ || Z == maxZ) 
			edgeZ = 1
		else 
			edgeZ = 0


		return (edgeX + edgeY + edgeZ) match {
  			case 0 => 27 //26 total cubes don't give right output, 27 did.
  			case 1 => 18 //1 face on edge, leaves 18 cubes
			case 2 => 12 //2 faces on edge, leaves 12 cubes
			case 3 => 8 //all 3 faces on edge leave 8 cubes

		}

		//return returnValue	


  }

def z_score(neighbors_value_sum: Int, mean: Double, num_of_neighbors: Int, std_dev: Double, num_of_cells: Int): Double =
	{
		var num = 0.0
		var denom = 0.0

		num = mean * num_of_neighbors.toDouble
		num = neighbors_value_sum - num

		denom = num_of_cells.toDouble * num_of_neighbors.toDouble - (num_of_neighbors.toDouble * num_of_neighbors.toDouble)
		denom = denom / (num_of_cells.toDouble - 1)
		denom = std_dev * math.sqrt(denom)
		
		return num / denom

	}

}

