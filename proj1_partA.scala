/*
Write a Scala script that identiﬁes the sub-populations with more than 90 members.
This is a chance to get familiar with using idioms like tmap and filter instead of loops to process data.
val panelfile = "integrated_call_samples_v3.20130502.ALL.panel" import scala.io.Source val biggroups = Source.fromFile(panelfile).getLines().drop(1)...
*/
import scala.io.Source
import org.bdgenomics.adam.rdd.ADAMContext._
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf

//object HelloWorld {
//  def main(args: Array[String]) {

//alias adam-submit="/Users/swaroopakkineni/adam/bin/adam-submit"
//alias adam-shell="/Users/swaroopakkineni/adam/bin/adam-shell"

//look at cluster performance by the amount of cores(1,2,4,8,16,32,64,100)
//spark-shell  --master local(N) runs locally on n number of cores
  //time loop on code from bigin

println("Hello World!!!!")
println(" ------------------------------------  Part 2 --------------------------------------------------- ")

val panelfile = "file_2a.txt"
//val biggroup = Source.fromFile(panelfile).getLines().drop(1).map(_.split("\t")).map(_(1)).countByValue().filter(_._2 >= 90)
val biggroups = sc.textFile(panelfile).map(_.split("\t")).map(_(1)).countByValue().filter(_._2 >= 90)
val keys = biggroups.keys
val values = biggroups.values
println(" ------------------------------------  Part 3 --------------------------------------------------- ")
val people = sc.textFile(panelfile).map(_.split("\t")).map( x => (x(0), x(1)) ).filter(x => keys.toList.contains(x._2)).collect()
println(" ------------------------------------  Part 4 --------------------------------------------------- ")
println("Populations with more than 90 individuals: "+biggroups.size)
println("Individuals from these populations: "+people.size)

/*println(" -------------------------------------------------------------------------------------------- ")
println(" -------------------------------------------------------------------------------------------- ")
println(" ------------------------------------  Section 3 --------------------------------------------------- ")
val data = sc.loadGenotypes("small.adam")

val map = data.rdd.map(r => ( (r.contigName, r.sampleId) , 1) ).countByValue()//.filter(_.2 == 1)
println("Total variants:" +map.size )
*/

/*val data = sc.loadGenotypes("small.adam")
println(data.rdd.take(1)(0))
** all we care about is { contigName,
                          start,
                          end, (start & end: uniquely define the variant position)
                          sampleID,(who has this variant)
                          alleles (what this variant is) }
**

adam-submit vcf2adam 1.10000-80000.ALL.chr1.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz small.adam


import collection.JavaConverters._
def convertAlleles(x: java.util.List[org.bdgenomics.formats.avro.GenotypeAllele] )= { x.asScala.map(_.toString) }

data.rdd.map(r => (r.contigName,r.start, r.end, r.sampleId, convertAlleles(r.alleles))).take(1)
//take(1) takes the top values

println(" ------------------------------------  Section 3 ------------------------------------------------ ")

val data = sc.loadGenotypes("small.adam")
println("|||||||||||||||| ")
import collection.JavaConverters._
def convertAlleles(x: java.util.List[org.bdgenomics.formats.avro.GenotypeAllele] )= { x.asScala.map(_.toString) }
//val mappedRDD = data.rdd.map(r => (r.contigName,r.start, r.end, r.sampleId, convertAlleles(r.alleles)))
val mappedVariants = data.rdd.map(x => (x.sampleId) ).countByValue()//.filter((_._2 <= 1))

println(" |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||| ")







*/








//
