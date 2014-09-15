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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.InputFormatInfo

import org.apache.hadoop.io.Text

import scopt.OptionParser
import scala.math.random
import scala.collection.mutable._

case class WordCountConfig(input: String = "",
                           output: String = "",
                           minSplits: Int = 1)

object WordCount {

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[WordCountConfig]("WordCount") {
      arg[String]("input") valueName("input") action {
        (x, c) => c.copy(input = x)
      }

      arg[String]("output") valueName("output") action {
        (x, c) => c.copy(output = x)
      }

      opt[Int]('s', "minSplits") valueName("minSplists") action {
        (x, c) => c.copy(minSplits = x)
      }

    }

    parser.parse(args, WordCountConfig()) map { config =>

      val sparkConf = new SparkConf()
      val sc = new SparkContext(sparkConf)

      val wordCount = new WordCount(sc, config.input, config.output, config.minSplits)

      sc.stop()
    } getOrElse {
      System.exit(1)
    }

  } 
}

@SerialVersionUID(1L)
class WordCount(sc: SparkContext, input: String,  output: String, minSplits: Int)
                                                 extends Serializable {

  val file = sc.textFile(input)

  // We first pick up value of key-value, and split sentences by space.
  // Next, words are counted up.
  val counts = file.map(line => line.split("\t")(1))
                    .flatMap(line => line.split(" "))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _)
                    .cache()

  counts.saveAsTextFile(output)

  def getNumWords() = {
    counts.count()
  }
}

// vim: ft=scala tw=0 sw=2 ts=2 et
