/*
Write a Scala script that identiﬁes the sub-populations with more than 90 members.
This is a chance to get familiar with using idioms like tmap and filter instead of loops to process data.
val panelfile = "integrated_call_samples_v3.20130502.ALL.panel" import scala.io.Source val biggroups = Source.fromFile(panelfile).getLines().drop(1)...
*/
import scala.io.Source
import org.bdgenomics.adam.rdd.ADAMContext._
import collection.JavaConverters._
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf

//object HelloWorld {
//  def main(args: Array[String]) {

//alias adam-submit="/Users/swaroopakkineni/adam/bin/adam-submit"
//alias adam-shell="/Users/swaroopakkineni/adam/bin/adam-shell"

//look at cluster performance by the amount of cores(1,2,4,8,16,32,64,100)
//spark-shell  --master local(N) runs locally on n number of cores
  //time loop on code from bigin

/* Sample code provided by assignment
// data.rdd.map(r => (r.contigName,r.start, r.end, r.sampleId, convertAlleles( r.alleles))).take(1)
*/

def convertAlleles(x: java.util.List[org.bdgenomics.formats.avro.GenotypeAllele] ) : Int =
{
    val bob = x.asScala.map(_.toString)
    if(bob(0) == "Alt" && bob(1) == "Alt"){
      return 2
    }
    else if(bob(0) == "Ref" && bob(1) == "Ref"){
      return 0
    }
    return 1
}

println("Hello World!!!!")
println(" ------------------------------------  Part 2 --------------------------------------------------- ")

val panelfile = "file_2a.txt"
//val biggroup = Source.fromFile(panelfile).getLines().drop(1).map(_.split("\t")).map(_(1)).countByValue().filter(_._2 >= 90)
val biggroups = sc.textFile(panelfile).map(_.split("\t")).map(_(1)).countByValue().filter(_._2 >= 90)
val keys = biggroups.keys
val values = biggroups.values
println(" ------------------------------------  Part 3 --------------------------------------------------- ")
val people = sc.textFile(panelfile).map(_.split("\t")).map( x => (x(0), x(1)) ).filter(x => keys.toList.contains(x._2)).collect()

//val peopleList = people.toList
//val variantsFrom_peopleList = peopleList.map(_._1)
println(" ------------------------------------  Part 4 --------------------------------------------------- ")
println("Populations with more than 90 individuals: "+biggroups.size)
println("Individuals from these populations: "+people.size)
println(" -------------------------------------------------------------------------------------------- ")
println(" -------------------------------------------------------------------------------------------- ")
println(" ------------------------------------  Section 3a --------------------------------------------------- ")
val adam = sc.loadGenotypes("small.adam")
println(" ////////////////////////////////////////////////////////////////////////////////////// ")

//val = adam.rdd.map(r => (r.contigName,r.start, r.end, r.sampleId, convertAlleles( r.alleles)))

val rawAdamData = adam.rdd.map(r =>  ( r.contigName, r.start, r.end, r.sampleId, convertAlleles(r.alleles)))

val varients = rawAdamData.map( r => ( r._2, r._3)).countByValue.filter(_._2 == 2504).map(_._1).toList
val varients_RDD_1 = sc.parallelize(varients).map(r => (r._1) )
val varients_RDD_2 = sc.parallelize(varients).map(r => (r._2) )
//varients_RDD_2.foreach(println)

val data = rawAdamData.filter( r => r._2 != varients_RDD_1 && r._3 != varients_RDD_2 )
//val take = data.take(1)



//val varList : List[(String, java.lang.Long, java.lang.Long, String, scala.collection.mutable.Buffer[String])] = rawData.collect().toList
//val varients = adam.rdd.map(r =>  ( r.contigName, r.start, r.end, r.sampleId )  ).countByValue()
//val filter = varients.filter(_._2 == 2504).toList








//
