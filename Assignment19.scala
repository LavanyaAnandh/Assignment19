import org.apache.spark.sql.SparkSession

object Assignment19 {
  case class Students_cls(name:String,Subject:String, grade:String,mark:Int,Id:Int )
  def main(args: Array[String]): Unit = {

    //Creating spark Session object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Assignment 19")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    println("Spark Session Object created")

    //Loading the 19_Dataset.txt file
    val data = spark.sparkContext.textFile("D:\\Lavanya\\19_Dataset.txt")
    println("Number of rows in the file 19_Dataset->>" + data.count) //counting the number of rows in the file

    //To get the number of rows in the given dataset
    val wordcount = data.flatMap(_.split(",")).count //counting the word seperated by ","
    println("Number of words in the file 19_Dataset->>" + wordcount)  //counting the number of words in the file

    //Loading the seperator.txt file which contains "-" as seperator
    val data1 = spark.sparkContext.textFile("D:\\Lavanya\\SeperatorFile.txt")
    val wordcount1 = data1.flatMap(_.split("-")).count //counting the word seperated by "-"
    println("Number of words in the File seperator->>" + wordcount1)  //counting the number of words in the file seperator which has "-" as seperator.

    //To get the number of rows in the tupled rdd
    val data3 = spark.read.textFile("D:\\Lavanya\\19_Dataset.txt").rdd
    data3.foreach(println)
    println("The number of rows in the rdd tuple data3 is " + data3.count())

    import spark.implicits._

    //converting the rdd into dataframe.
    val data4 = data3.map(x=>x.split(",")).map(x => Students_cls(x(0),x(1),x(2),x(3).toInt,x(4).toInt)).toDF()
    data4.show()
    data4.registerTempTable("StudentsMark") //Registering as temporary table HVAC.
    println("Dataframe Registered as table !")

    //To get the number of Distinct Subjects
    val DistinctSubject = spark.sql("select distinct(Subject) from StudentsMark").count()
    println("The number of Distinct Subjects are :" +  DistinctSubject)

    //To count the number of students having name as mathew and mark as 55
    val StudCount = spark.sql("select * from StudentsMark where name = 'Mathew' and mark = 55").count()
    println("The count of number of students having name as mathew and mark as 55 :" +  StudCount)

    //To get the count of students per grade in the school
    val AvgCountpergrade = spark.sql(sqlText= "select count(name), grade from StudentsMark group by grade")
    AvgCountpergrade.show()

    //To get the average of each students per grade in the school
    val AvgStudentsgrade = spark.sql(sqlText= "select name ,avg(mark), grade from StudentsMark group by grade, name")
    AvgStudentsgrade.show()

    //To get average score of students in each subject across all grades
    val AvgScoreperSubject = spark.sql(sqlText= "select name,avg(mark), subject from StudentsMark group by subject, name")
    AvgScoreperSubject.show()

    //To get What is the average score of students in each subject per grade?
    val AvgScoreinSubjectpergrade = spark.sql(sqlText= "select name,avg(mark),subject, grade from StudentsMark group by subject, name, grade")
    AvgScoreinSubjectpergrade.show()

    //To get how many have average score greater than 50 in grade-2?
    val AvgScoregreaterthan50 = spark.sql(sqlText= "select name ,avg(mark), grade from StudentsMark group by name, grade having grade = 'grade-2' and avg(mark) > 50 ")
    AvgScoregreaterthan50.show()

//  1. Average score per student_name across all grades is same as average score per student_name per grade Hint - Use Intersection Property */
    //To get the average of each students across grades
    val AvgStudentspergrade = spark.sql(sqlText= "select name ,avg(mark) from StudentsMark group by name")
    AvgStudentspergrade.show()
    val droppedAvgStudentsgrade =  AvgStudentsgrade.drop("grade")
    val finalunion = AvgStudentspergrade.intersect(droppedAvgStudentsgrade).count()
    println("Number of students who satisfy the given criteria: " + finalunion)
  }
}