/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package $package$

import java.util.Random
import scala.math.exp
import org.apache.spark.util.Vector
import org.apache.spark._

/**
 * Logistic regression based classification.
 */
object SparkLR {

  def main(args: Array[String]) {
    if (args.length == 0) {
	    System.err.println("Usage: SparkLR <master> [<slices>], [<iter>] [<num_datapoint>] [<dimensions>]")
	    System.err.println("")
	    System.err.println("  slices: The number of split")
	    System.err.println("  iter: The number of iterrating")
	    System.err.println("  num_datapoint: The number of data point")
	    System.err.println("  dimensions: The size of vectors")
      System.exit(1)
    }

    val numSlices = if (args.length > 1) args(1).toInt else 2
    val numIter = if (args.length > 2) args(2).toInt else 5
    val numDatapoint = if (args.length > 3) args(3).toInt else 10
    val dimensions = if (args.length > 4) args(4).toInt else 10

    val sc = new SparkContext(args(0), "SparkLR",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

    val sparkLR = new SparkLR(sc, numSlices, numIter, numDatapoint, dimensions)

    println("Final w: " + sparkLR.w)
    System.exit(0)
  }
}

@serializable
@SerialVersionUID(1L)
class SparkLR(sc: SparkContext, numSlices: Int, numIter: Int, numDatapoint: Int, dimensions: Int) {
  val ITERATIONS = numIter
  val N = numDatapoint  // Number of data points
  val D = dimensions   // Numer of dimensions
  val R = 0.7  // Scaling factor
  val rand = new Random(42)

  case class DataPoint(x: Vector, y: Double)

  def generateData = {
    def generatePoint(i: Int) = {
      val y = if(i % 2 == 0) -1 else 1
      val x = Vector(D, _ => rand.nextGaussian + y * R)
      DataPoint(x, y)
    }
    Array.tabulate(N)(generatePoint)
  }
    val points = sc.parallelize(generateData, numSlices).cache()

    // Initialize w to a random value
    var w = Vector(D, _ => 2 * rand.nextDouble - 1)
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradient = points.map { p =>
        (1 / (1 + exp(-p.y * (w dot p.x))) - 1) * p.y * p.x
      }.reduce(_ + _)
      w -= gradient
    }

}
