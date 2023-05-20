import org.apache.spark.{SparkConf, SparkContext}

object Graphs {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Graphs")
        val sc = new SparkContext(sparkConf) // create spark context

        val inputFile = "input/web-Stanford.txt"

        // Διάβασμα του αρχείου, αγνοώντας τις γραμμές που ξεκινάνε με #
        val graph = sc.textFile(inputFile)
            .filter(!_.startsWith("#"))

        // Ανάγνωση των ακμών του γράφου και δημιουργία του RDD
        val edges = graph.map(line => {
            val fields = line.split("\t")
            (fields(0).toInt, fields(1).toInt)
        })

        // Μέτρηση εισερχόμενων ακμών για κάθε κόμβο και εκτύπωση των 10 πρώτων
        val inDegrees = edges.map(edge => (edge._2, 1)) // Δημιουργία λίστας με key τον κόμβο που καταλήγει η ακμή και value 1
            .reduceByKey(_ + _)     // Άθροιση των εμφανίσεων του κόμβου
            .sortBy(_._2, ascending = false)    // Ταξινόμηση με βάση το value

        println("Κόμβοι με τις περισσότερες εισερχόμενες ακμές")

        inDegrees.take(10).foreach(println)

        // Μέτρηση εξερχόμενων ακμών για κάθε κόμβο και εκτύπωση των 10 πρώτων
        val outDegrees = edges.map(edge => (edge._1, 1))    // Δημιουργία λίστας με key τον κόμβο που ξεκινά η ακμή και value 1
            .reduceByKey(_ + _)   // Άθροιση των εμφανίσεων του κόμβου
            .sortBy(_._2, ascending = false)    // Ταξινόμηση με βάση το value

        println("\nΚόμβοι με τις περισσότερες εξερχόμενες ακμές")

        outDegrees.take(10).foreach(println)

        // Μέτρηση εισερχόμενων και εξερχόμενων (συνολικά) ακμών για κάθε κόμβο
        // Δημιουργία λίστας με τα δύο ζεύγη key-value (για κάθε κόμβο της ακμής). Τα δύο ζεύγη τοποθετούνται στη σειρά
        val degrees = edges.flatMap(edge => List((edge._1, 1), (edge._2, 1)))
            .reduceByKey(_ + _)  // Άθροιση των εμφανίσεων του κόμβου

        // Υπολογισμός μέσου όρου του πλήθους των ακμών
        val avg = degrees.map(_._2).mean()

        println(f"\nΜέσος όρος ακμών: $avg%.2f")

        // Υπολογισμός του πλήθους των κόμβων που έχουν περισσότερες (ή ίσες) από avg ακμές
        val moreThanAvg = degrees.filter(_._2 >= avg).count()

        println(f"\nΠλήθος κόμβων με περισσότερες από $avg%.2f ακμές: $moreThanAvg")
    }
}
