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
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.InputFormatInfo

/**
 * Logistic regression based classification.
 */
object SparkHdfsLR {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: SparkHdfsLR <file> <iters> <dimensions>")
      System.exit(1)
    }

    val inputPath = args(0)
    val num_iter = args(1).toInt
    val dimensions = args(2).toInt

    val sparkConf = new SparkConf().setAppName("SparkHdfsLR")
    val sc = new SparkContext(sparkConf)

    val sparkHdfsLR = new SparkHdfsLR(sc, inputPath, num_iter, dimensions)

    println("Final w: " + sparkHdfsLR.w)
    sc.stop()
  }
}

@serializable
@SerialVersionUID(1L)
class SparkHdfsLR(sc: SparkContext, inputPath: String, num_iter: Int, dimensions: Int) {
  val D = dimensions   // Numer of dimensions
  val rand = new Random(42)

  case class DataPoint(x: Vector, y: Double)

  def parsePoint(line: String): DataPoint = {
    //val nums = line.split(' ').map(_.toDouble)
    //return DataPoint(new Vector(nums.slice(1, D+1)), nums(0))
    val tok = new java.util.StringTokenizer(line, " ")
    var y = tok.nextToken.toDouble
    var x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = tok.nextToken.toDouble; i += 1
    }
    DataPoint(new Vector(x), y)
  }

  val lines = sc.textFile(inputPath)
  val points = lines.map(parsePoint _).cache()

  // Initialize w to a random value
  var w = Vector(D, _ => 2 * rand.nextDouble - 1)
  println("Initial w: " + w)

  for (i <- 1 to num_iter) {
    println("On iteration " + i)
    val gradient = points.map { p =>
      (1 / (1 + exp(-p.y * (w dot p.x))) - 1) * p.y * p.x
    }.reduce(_ + _)
    w -= gradient
  }
}

// vim: tw=0 et sw=2 ts=2
