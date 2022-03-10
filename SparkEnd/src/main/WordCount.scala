
object WordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WorkCount")
      .setMaster("spark://114.132.227.132:7077")



    val sc = new SparkContext(conf)
    //sc.addJar("E:\\sunxj\\idea\\spark\\out\\artifacts\\spark_jar\\spark.jar")
    //val line = sc.textFile(args(0))
    val file=sc.textFile("hdfs://114.132.227.132:9000/user/Wordcount.txt")
    val rdd = file.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
    rdd.collect()
    rdd.foreach(println)
    rdd.collectAsMap().foreach(println)
  }
}