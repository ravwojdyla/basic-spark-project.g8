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

case class WordCountConfig(master: String = "",
                           input: String = "",
                           output: String = "",
                           userName: String = "spark",
                           minSplits: Int = 1,
                           inputFormat: String = "textFile")

object WordCount {

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[WordCountConfig]("WordCount") {
      opt[String]('u', "userName") valueName("userName") action {
        (x, c) => c.copy(userName = x)
      }

      arg[String]("master") valueName("master") action {
        (x, c) => c.copy(master = x)
      }

      arg[String]("input") valueName("input") action {
        (x, c) => c.copy(input = x)
      }

      arg[String]("output") valueName("output") action {
        (x, c) => c.copy(output = x)
      }

      opt[Int]('s', "minSplits") valueName("minSplists") action {
        (x, c) => c.copy(minSplits = x)
      }

      opt[String]('i', "inputFormat") valueName("inputFormat") action {
        (x, c) => c.copy(inputFormat = x)
      }

    }

    parser.parse(args, WordCountConfig()) map { config =>

      val inputFormatClass = if (config.inputFormat == "textFile")
                               classOf[org.apache.hadoop.mapred.TextInputFormat]
                             else
                               classOf[org.apache.hadoop.mapred.SequenceFileInputFormat[Text,Text]]

      val hadoopConf = SparkHadoopUtil.get.newConfiguration()

      val sc = new SparkContext(config.master, "WordCount", System.getenv("SPARK_HOME"),
                     SparkContext.jarOfClass(this.getClass()),
                     Map(),
                     InputFormatInfo.computePreferredLocations(
                       Seq(new InputFormatInfo(hadoopConf, inputFormatClass, config.input))))

      val wordCount = new WordCount(sc, config.input, config.output, config.minSplits)

      sc.stop()
      System.exit(0)
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
