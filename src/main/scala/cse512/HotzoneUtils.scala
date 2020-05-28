package cse512

object HotzoneUtils {

  def ST_Contains(queryRect: String, pointStr: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    
  

    //Check if the point is empty or null, then return false
    if(pointStr==null || pointStr.isEmpty())
      return(false)
    //check if the rectangle is empty or null, then return false
    if(queryRect==null || queryRect.isEmpty())
      return(false)


    // Coordinates of the point
    val point = pointStr.split(",")
    var x = point(0).toDouble
    var y = point(1).toDouble

    
    // Coordinates of the query rectangle (x1,y1), (x2,y2)
    val rectangle = queryRect.split(",")
    val x1 = rectangle(0).toDouble
    val y1 = rectangle(1).toDouble
    val x2 = rectangle(2).toDouble
    val y2 = rectangle(3).toDouble

    var min_x = if (x1 < x2) x1 else x2 
    var max_x = if (x1 > x2) x1 else x2 
    var min_y = if (y1 < y2) y1 else y2
    var max_y = if (y1 > y2) y1 else y2
    
    if(x >= min_x && x <= max_x && y >= min_y && y <= max_y) {
      return(true)
    } else {
      return(false)
    }

}
}
