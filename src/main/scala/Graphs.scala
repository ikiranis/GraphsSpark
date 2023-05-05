import org.apache.spark.{SparkConf, SparkContext}

object Graphs {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Graphs")
        val sc = new SparkContext(sparkConf) // create spark context

        val inputFile = "input/web-Stanford.txt"
//        val inputFile = "input/text.txt"

        val graph = sc.textFile(inputFile)
            .filter(!_.startsWith("#"))

        val edges = graph.map(line => {
            val fields = line.split("\t")
            (fields(0).toInt, fields(1).toInt)
        })

        println("Κόμβοι με τις περισσότερες εισερχόμενες ακμές")

        // Μέτρηση εισερχόμενων ακμών για κάθε κόμβο και εκτύπωση των 10 πρώτων
        val inDegrees = edges.map(edge => (edge._2, 1))
            .reduceByKey(_ + _)
            .sortBy(_._2, ascending = false)

        inDegrees.take(10).foreach(println)

        println("Κόμβοι με τις περισσότερες εξερχόμενες ακμές")

        // Μέτρηση εξερχόμενων ακμών για κάθε κόμβο και εκτύπωση των 10 πρώτων
        val outDegrees = edges.map(edge => (edge._1, 1))
            .reduceByKey(_ + _)
            .sortBy(_._2, ascending = false)

        outDegrees.take(10).foreach(println)

    }
}
