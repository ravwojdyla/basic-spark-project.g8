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
import org.apache.spark.storage.StorageLevel
import java.util.Random
import java.lang.Thread

object GroupByTest {
  // Usage: GroupByTest [numMappers] [numKVPairs] [valSize] [numReducers]
  def main(args: Array[String]) {
    var numMappers = if (args.length > 0) args(0).toInt else 2
    var numKVPairs = if (args.length > 1) args(1).toInt else 1000
    var valSize = if (args.length > 2) args(2).toInt else 1000
    var numReducers = if (args.length > 3) args(3).toInt else numMappers
    var storageLevel = StorageLevel.MEMORY_ONLY
    var sleeptime = if (args.length > 5) args(5).toInt else 0

    if (args.length > 4) {
      if (args(4) == "MEMORY_ONLY") storageLevel = StorageLevel.MEMORY_ONLY
      if (args(4) == "MEMORY_AND_DISK") storageLevel = StorageLevel.MEMORY_AND_DISK
      if (args(4) == "DISK_ONLY") storageLevel = StorageLevel.DISK_ONLY
    }

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    Thread.sleep(sleeptime)

    val groupByTest = new GroupByTest(sc, numMappers, numKVPairs, valSize, numReducers, storageLevel)

    println("total count: " + groupByTest.count)

    sc.stop()
  }
}

@serializable
@SerialVersionUID(1L)
class GroupByTest (sc: SparkContext, numMappers: Int, numKVPairs: Int, valSize: Int, numReducers: Int, storageLevel: StorageLevel) {

    def generateData = {
      val ranGen = new Random
      val iter = new Iterator[Pair[Int,Array[Byte]]] {
        private var i = 0
        def hasNext = i < numKVPairs
        def next = {
          if (i < numKVPairs) {
            i += 1
            val byteArr = new Array[Byte](valSize)
            ranGen.nextBytes(byteArr)
            Pair(ranGen.nextInt(Int.MaxValue), byteArr)
          } else error("next on empty iterator")
        }
      }
      iter
    }

    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
    generateData
  }.persist(storageLevel)

    // Enforce that everything has been calculated and in cache
    pairs1.count

    val count = pairs1.groupByKey(numReducers).count
}

