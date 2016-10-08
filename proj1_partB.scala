import scala.io.Source
import org.bdgenomics.adam.rdd.ADAMContext._
import collection.JavaConverters._
val data = sc.loadGenotypes("small.adam")
/*
println(" -------------------------------------Part 2B------------------------------------------------ ")
println(" -------------------------------------------------------------------------------------------- ")
val data = sc.loadGenotypes("small.adam")
println(data.rdd.take(1))
def convertAlleles(x: java.util.List[org.bdgenomics.formats.avro.GenotypeAllele] ) = {
  x.asScala.map(_.toString)
}
data.rdd.map(r => (r.contigName,r.start, r.end, r.sampleId, convertAlleles(r.alleles))).take(1)
println(" -------------------------------------------------------------------------------------------- ")
println(" -------------------------------------------------------------------------------------------- ")
*/
println(" -------------------------------------------------------------------------------------------- ")
println(" -------------------------------------------------------------------------------------------- ")
//val gv = data.rdd.map(r => ((r(1),r(2), r(3), r(4) ), 1) )//mapping each value individually to 1
def convertAlleles(x: java.util.List[org.bdgenomics.formats.avro.GenotypeAllele] ) = { x.asScala.map(_.toString) }
println(" -------------------------------------------------------------------------------------------- ")
val map = data.rdd.map(r => ( (r.contigName, r.sampleId) , 1) ).countByValue().filter(_.2 == 1)

println("Total variants:" +map.size )
//.reduceByKey(_ + _)
//val variants = map.countByValue()// adds to values, http://stackoverflow.com/questions/24071560/using-reducebykey-in-apache-spark-scala
//val goodVariants = variants.filter(_._2 == 1)//less than 1






























//
