package org.apache.maven

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

/*
Parse the input xml files to calculate the pageranks for all the pages in the given input
 */

object App {
  //This function parses the bx2 file to generate the adjacency lists including all the dangling nodes
  def parse(sc: SparkContext, input: String) : RDD[(String,List[String])] ={
    // the initial adjacency list without any dangling nodes. This is of the format RDD[(String,List[String])].
    // With pagename along with the links in that page
    val tempList = sc.textFile(input)
      .map(line => {
        val pages = Parser.map(line) // call the java code from previous assignment to parse each line
        var pageName = ""
        var links = List("")
        // if the returned output is not empty, add the adjacency list to the rdd
        if(pages.size() > 0) {
          // output from the parser has the page name as the first element in the list. So extract the page name
          // and it's adjacency list
          pageName = pages.getFirst
          if (pages.size() > 1) {
            val l = List("")
            links = pages.subList(1, pages.size()).asScala.toList
            // emit each link in the adjacency list with an empty adjacency list to find the dangling nodes
            links.map(link => (link,l))
          }
        }
        (pageName,links) // add the adjacency list for this page to the RDD
      }).filter(page => !page._1.isEmpty)
      // reduce the whole RDD by key and combine the lists for the same page to get the final list with all the
      // dangling nodes
      .reduceByKey((list1,list2) => {val list = list1 ::: list2
        list.filter(_.nonEmpty)
      }).persist(StorageLevel.MEMORY_AND_DISK) // store this RDD in memory since we access it for further processing
    tempList
  }

  // calculate page ranks for all the pages in the adjacency list
  def calcPageRank(sc: SparkContext, adjList: RDD[(String,List[String])]): RDD[(String,(List[String],Double))] ={
    val totalNodes = adjList.count().toDouble
    val initialPageRank =1.0/totalNodes
    val alpha = 0.85
    val beta = 0.15
    // add the initial page rank to all the pages in the adjacency list
    var pageRanks = adjList.map(page=>(page._1,(page._2,initialPageRank)))
    // delta accumulator
    val delta = sc.doubleAccumulator
    for(i <- 1 to 10){
      // iterate through all the pages and calculate the page rank for each link in the adjacency list of each page
      val tempRanks = pageRanks.values.flatMap(pages =>{if(pages._1.isEmpty){
                                                        delta.add(pages._2) // if the page has no links, update delta
                                                      }
                                                        // else calculate the intermediate page rank for each link in
                                                        // adjacency list and add to the RDD
                                                        pages._1.map(page=>(page,pages._2/pages._1.size))
                                                      }).reduceByKey(_+_)

      //  the intermediate page ranks are appended to the original RDD with all the pages
      // RDD [pageName, (adjacencyList, intermediatePageRank)]
      val ranks = adjList.leftOuterJoin(tempRanks).map(page => (page._1, (page._2._1,page._2._2.getOrElse(0.0))))

      // calculate the page rank for all the pages using the intermediate page ranks calculated
      pageRanks = ranks.map(page=>{val r = alpha*(1/totalNodes)+beta*((delta.value/totalNodes)+page._2._2)
        (page._1,(page._2._1,r))
      }).filter(page => !page._1.isEmpty)
      delta.reset() // reset the delta value for next calculation
    }
    pageRanks
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Page Rank").setMaster("yarn")
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    val adjList = parse(sc,input) // adjacency list returned form the function
    val pageRanks = calcPageRank(sc,adjList) // page rank returned from the function
//
    // emit the top 100 pages from the page ranks RDD by sorting it by the ranks and emitting the top 100
    val top100 = sc.parallelize(pageRanks.map(page => (page._2._2,page._1)).sortByKey(false).take(100))
    // save that rdd to the output file
    top100.saveAsTextFile(output)
    sc.stop()
  }
}