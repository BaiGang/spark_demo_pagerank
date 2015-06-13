//  Some license.
//  Author who.

package com.ibm.bigdata.pagerank

import scopt.OptionParser
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging

object PageRank extends Logging {

  // definition of program arguments
  case class Params(
    appName: String = "demo",
    master: String = null,
    numIters: Int = 10,
    fromFile: Boolean = false,
    linksPath: String = null,
    resultPath: String = null)

  def argumentsParser: OptionParser[Params] = new OptionParser[Params]("PageRank") {
    head("PageRank demo...")
    opt[String]("app_name")
      .text("Applicaton name")
      .action((x, c) => c.copy(appName = x))
    opt[Int]("num_iters")
      .text("Num of iterations")
      .action((x, c) => c.copy(numIters = x))
    opt[String]("links_file")
      .text("File containing the links of each page.")
      .required()
      .action((x, c) => c.copy(linksPath = x))
    opt[String]("result_path")
      .text("Path to the pagerank results.")
      .required()
      .action((x, c) => c.copy(resultPath = x))
  }

  def main(args: Array[String]) {
    argumentsParser.parse(args, Params()) map {
      case params: Params =>
        run(params)
    } getOrElse {
      // something's wrong
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(params.appName)
    // master url is set via --master in bin/spark-submit
    val sc = new SparkContext(conf)

    // formated as "pageId linkstoId1,linkstoId2,linkstoId3"
    val links = sc.textFile(params.linksPath).map(line => { val x = line.split(" "); (x(0), x(1).split(",")) })

    // initial ranks 1.0
    var ranks = links.map { case (page, links) => (page, 1.0) }

    for (iter <- 1 to params.numIters) {
      val contribs = links.join(ranks).flatMap {
        case (page, (links, rank)) =>
          links.map(linkto => (linkto, rank / links.size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    // done
    ranks.saveAsTextFile(params.resultPath)
  }

}

