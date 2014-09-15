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

import org.apache.spark.rdd.SequenceFileRDDFunctions
import org.apache.spark.SparkContext._

import org.apache.hadoop.io.NullWritable

/**
 * Logistic regression based classification.
 */
object SparkLRTestDataGenerator {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkLRTestDataGenerator <output> [<output_mode>] [<parallel>], [<num_datapoint>] [<dimensions>]")
	    System.err.println("")
	    System.err.println("  output_mode: The output format. textfile, sequencefile, objectfile (default: textfile)")
	    System.err.println("  parallel: The number of parallel degree (default: 2)")
	    System.err.println("  num_datapoint: The number of data point (default: 10)")
	    System.err.println("  dimensions: The size of vectors (default: 10)")
      System.exit(1)
    }

    val output = args(0)
    val outputMode = if (args.length > 1) args(1) else "textfile"
    val parallel = if (args.length > 2) args(2).toInt else 2
    val numDatapoint = if (args.length > 3) args(3).toInt else 10
    val dimensions = if (args.length > 4) args(4).toInt else 10

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val sparkLRTestDataGenarator = new SparkLRTestDataGenerator(sc, output,  parallel, numDatapoint, dimensions)

    if (outputMode == "textfile") {
      sparkLRTestDataGenarator.saveTestDataAsTextFile()
    } else if (outputMode == "sequencefile") {
      sparkLRTestDataGenarator.saveTestDataAsSequenceFile()
    } else {
      println("The wrong format: " + outputMode)
    }

    sc.stop()
  }
}

@serializable
@SerialVersionUID(1L)
class SparkLRTestDataGenerator(sc: SparkContext, output: String, parallel: Int = 2, numDatapoint: Int = 10, dimensions: Int = 10) {
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
    //Array.tabulate(N)(generatePoint)
    val iter = new Iterator[DataPoint] {
      private var i = 0
      def hasNext = i < N
      def next = {
        if (i < N) { val dp = generatePoint(i); i += 1;  dp} else error("next on empty iterator")
      }
    }

    iter
  }

  val points = sc.parallelize(1 to parallel, parallel).flatMap { p =>
    generateData
  }

  def saveTestDataAsTextFile() = {
    points.map { p =>
      p.y + " " + p.x.toString().replace("(", "").replace(")", "").replace(",", "")
    }.saveAsTextFile(output)
  }

  def saveTestDataAsSequenceFile() = {
    points.map { p=>
      (NullWritable.get, p.y + " " + p.x.toString().replace("(", "").replace(")", "").replace(",", ""))
    }.saveAsSequenceFile(output)
  }

}

// vim: et tw=0 ts=2 sw=2
