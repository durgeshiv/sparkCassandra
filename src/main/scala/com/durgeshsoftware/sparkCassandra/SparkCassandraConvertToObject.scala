package com.durgeshsoftware.sparkCassandra

import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions

/**
 * Use case class
 * Retrieve all books with genre “crime”
 * Output format should be Author name: Book Name (publish year) [rating]
 * 
 */


case class Books(author_name:String,book_name:String,publish_year:Int,rating:Option[Float])

object SparkCassandraConvert {
  
    def main(args: Array[String]): Unit = {
      
       val conf = new SparkConf()
      conf.set("spark.cassandra.connection.host", "localhost")
      conf.setMaster("local[*]")
      conf.setAppName("Spark Cassandra Integration")
      
     
      val sc = new SparkContext(conf)
      val books = sc.cassandraTable[Books]("testkeyspace","books_by_author")
	                  .select("author_name","book_name","publish_year","rating")

      
      books.foreach(println)
	                  
	    /*val filteredBooks = books.filter { book  => book.author_name.contains("2018")}
      filteredBooks.map{book =>
                              book.author_name+":"+
                              book.book_name+
                              "("+ book.publish_year+")"+
                               "["+book.rating.getOrElse("No Rating")+"]"
                    }.collect.foreach(println)*/
    }
}