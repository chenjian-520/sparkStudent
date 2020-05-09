package com.aura.bigdata.spark.scala.sql.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
/*
现有如此三份数据：
1、users.dat    数据格式为：  2::M::56::16::70072
对应字段为：UserID BigInt, Gender String, Age Int, Occupation String, Zipcode String
对应字段中文解释：用户id，性别，年龄，职业，邮政编码

2、movies.dat		数据格式为： 2::Jumanji (1995)::Adventure|Children's|Fantasy
对应字段为：MovieID BigInt, Title String, Genres String
对应字段中文解释：电影ID，电影名字，电影类型

3、ratings.dat		数据格式为：  1::1193::5::978300760
对应字段为：UserID BigInt, MovieID BigInt, Rating Double, Timestamped String
对应字段中文解释：用户ID，电影ID，评分，评分时间戳
 */
object _01MovieTest {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val spark = SparkSession.builder()
            .master("local[4]")
            .appName("_01MovieTest")
            .getOrCreate()

        spark.udf.register[Int, String]("getYear", getYear)
        spark.udf.register[String, String]("getMovieName", getMovieName)

        createUsers(spark)
        createMovies(spark)
        createRating(spark)

//        1、求被评分次数最多的10部电影，并给出评分次数（电影名，评分次数）
//        movieRatingTop10(spark)
//        2、分别求男性，女性当中评分最高的10部电影（性别，电影名，影评分）
//        movieRatingGenderTop10(spark)
//        3、分别求男性，女性看过最多的10部电影（性别，电影名）
//        movieCountGenderTop10(spark)
//        4、年龄段在“18-24”的男人，最喜欢看10部电影
//        ageStateTop10(spark)
//       5、求movieid = 2116这部电影各年龄段（因为年龄就只有7个，就按这个7个分就好了）的平均影评（年龄段，影评分）
//        method5(spark)
//        6、求最喜欢看电影（影评次数最多）的那位女性评最高分的10部电影的平均影评分（观影者，电影名，影评分）
//        method6(spark)
//        7、求好片（评分>=4.0）最多的那个年份的最好看的10部电影
           method7(spark)
//        8、求1997年上映的电影中，评分最高的10部Comedy类电影
//            method8(spark)
//        9、该影评库中各种类型电影中评价最高的5部电影（类型，电影名，平均影评分）
//
//        10、各年评分最高的电影类型（年份，类型，影评分）

        spark.stop()
    }

    /*
        求1997年上映的电影中，评分最高的10部Comedy类电影
     */
    def method8(spark:SparkSession): Unit = {
        println(">>>>>>>求1997年上映的电影中，评分最高的10部Comedy类电影")

        val sql =
            """
              |
            """.stripMargin

        spark.sql(sql).show()
    }
    def getYear(str:String): Int = {
        str.substring(str.lastIndexOf("(") + 1, str.lastIndexOf(")")).toInt
    }
    def getMovieName(str: String):String = {
        str.substring(0, str.lastIndexOf("("))
    }
    /**
      * 求好片（评分>=4.0）最多的那个年份的最好看的10部电影
      *
      * @param spark
      */
    def method7(spark:SparkSession): Unit = {
        println(">>>>>>>求好片（评分>=4.0）最多的那个年份的最好看的10部电影")
        val sql =
            """
              |select
              | r.movie_id,
              | t2.movie_name,
              | t2.year,
              | count(1) mcount
              |from (
              |select
              |     m.movie_id,
              |     getMovieName(m.movie_name) movie_name,
              |     t1.year
              |from movie m ,
              |(
              |  select
              |     tmp.year,
              |     count(1) as ycount
              |  from (
              |   select
              |      r.movie_id,
              |      getYear(m.movie_name) year
              |   from rating r
              |   left join movie m on r.movie_id = m.movie_id
              |   where r.score >= 4
              |  ) tmp
              |  group by tmp.year
              |  order by ycount desc limit 1
              |) t1 where t1.year = getYear(m.movie_name)
              |) t2
              |left join rating r on t2.movie_id = r.movie_id
              |group by r.movie_id, t2.movie_name, t2.year
              |order by mcount desc
              |limit 10
            """.stripMargin
        spark.sql(sql).show()
    }
    /*
       6、求最喜欢看电影（影评次数最多）的那位女性评最高分的10部电影的平均影评分
       （观影者，电影名，影评分）
     */
    def method6(spark:SparkSession): Unit = {
        println(">>>>>>>最喜欢看电影（影评次数最多）的那位女性评最高分的10部电影的平均影评分（观影者，电影名，影评分） ")

        val sql =
            """
              |select
              |  r.movie_id,
              |  m.movie_name,
              |  bround(avg(r.score), 2) avg_score
              |from (
              |  select
              |    t1.user_id,
              |    r.score,
              |    r.movie_id
              |  from (
              |      select
              |          r.user_id,
              |          count(1) as countz
              |      from rating r
              |      group by r.user_id
              |      order by countz desc
              |      limit 1
              |  ) t1
              |  left join rating r on t1.user_id = r.user_id
              |  order by r.score desc
              |  limit 10
              | ) t2
              | left join rating r on r.movie_id = t2.movie_id
              | left join movie m on t2.movie_id = m.movie_id
              | group by r.movie_id, m.movie_name
            """.stripMargin
        spark.sql(sql).show()
    }
    /*
    5、求movieid = 2116这部电影各年龄段的平均影评（年龄段，影评分）
        年龄段：
            18~25 26~30 31~40 41~50 51~60
     */
    def method5(spark:SparkSession): Unit = {
        println(">>>>>>>求movieid = 2116这部电影各年龄段（因为年龄就只有7个，就按这个7个分就好了）的平均影评（年龄段，影评分）")
        val sql =
            """
              |select
              |  tmp.age_stage,
              |  bround(avg(tmp.score), 2) avg_score
              |from (
              |select
              |  age,
              |  (case
              |     when age between 18 and 25 then "age_18_25"
              |     when age between 26 and 30 then "age_26_30"
              |     when age between 31 and 40 then "age_31_40"
              |     when age between 41 and 50 then "age_41_50"
              |     when age between 51 and 60 then "age_51_60"
              |     else "other"
              |  end) age_stage,
              |  r.score
              |from rating r
              |left join user u on r.user_id = u.user_id
              |) tmp
              |group by age_stage
            """.stripMargin
        spark.sql(sql).show()
    }
    /**
      * 4、年龄段在“18-24”的男人，最喜欢看10部电影
      */
    def ageStateTop10(spark:SparkSession): Unit = {
        println(">>>>>>>年龄段在“18-24”的男人，最喜欢看10部电影")
        val sql =
            """
              | select
              |    r.movie_id,
              |    m.movie_name,
              |    count(r.movie_id) countz
              | from rating r
              | left join user u on r.user_id = u.user_id
              | left join movie m on r.movie_id = m.movie_id
              | where u.gender = 'M'
              | and age between 18 and 24
              | group by r.movie_id, m.movie_name
              | order by countz desc
              | limit 10
            """.stripMargin
        spark.sql(sql).show()
    }
    /**
      * 3、分别求男性，女性看过最多的10部电影（性别，电影名）
      */
    def  movieCountGenderTop10(spark:SparkSession): Unit = {
        println(">>>>>>>分别求男性，女性看过最多的10部电影（性别，电影名）")
        val sql =
            """
              | select
              |   tmp.gender ,
              |   tmp.movie_name,
              |   tmp.countz,
              |   row_number() over(partition by gender order by tmp.countz desc) rank
              | from (
              | select
              |    u.gender,
              |    r.movie_id,
              |    m.movie_name,
              |    count(r.movie_id) countz
              | from rating r
              | left join user u on r.user_id = u.user_id
              | left join movie m on r.movie_id = m.movie_id
              | group by r.movie_id, m.movie_name, u.gender
              | ) tmp
              | having rank < 11
            """.stripMargin
        spark.sql(sql).show()
    }
    /**
      * 2、分别求男性，女性当中评分最高的10部电影（性别，电影名，影评分）
      * @param spark
      */
    def movieRatingGenderTop10(spark:SparkSession): Unit = {
        println(">>>>>>>分别求男性，女性当中评分最高的10部电影（性别，电影名，影评分）")
        val sql =
            """
              |select
              |  tmp.*
              |from (
              | select
              |    u.gender,
              |    m.movie_name,
              |    r.score,
              |    row_number() over(partition by u.gender order by r.score desc) rank
              | from rating r
              | left join user u on r.user_id = u.user_id
              | left join movie m on r.movie_id = m.movie_id
              |) tmp
              |where rank < 11
            """.stripMargin
        spark.sql(sql).show()
    }
    /**
      * 1、求被评分次数最多的10部电影，并给出评分次数（电影名，评分次数）
      * @param spark
      */
    def movieRatingTop10(spark:SparkSession): Unit = {
        println(">>>>>>>评分次数最多的10部电影，并给出评分次数（电影名，评分次数）")
        val sql =
            """
              |select
              |  r.movie_id,
              |  getMovieName(m.movie_name) movie_name,
              |  count(r.movie_id) as countz
              |from rating r
              |left join movie m on r.movie_id = m.movie_id
              |group by r.movie_id, m.movie_name
              |order by countz desc
              |limit 10
            """.stripMargin

        spark.sql(sql).show()
    }
    /*
        3、ratings.dat		数据格式为：  1::1193::5::978300760
        对应字段为：UserID BigInt, MovieID BigInt, Rating Double, Timestamped String
        对应字段中文解释：用户ID，电影ID，评分，评分时间戳
     */
    def createRating(spark:SparkSession): Unit = {
        val sc = spark.sparkContext
        val lines = sc.textFile("data/test/ratings.dat")
        val ratingRDD = lines.map(line => {
            val fields = line.split("::")
            val userId = fields(0).toInt
            val movieId = fields(1).toInt
            val score = fields(2).toDouble
            val time = fields(3).toLong
            Row(userId, movieId, score, time)
        })
        val schema = StructType(List(
            StructField("user_id", DataTypes.IntegerType, false),
            StructField("movie_id", DataTypes.IntegerType, false),
            StructField("score", DataTypes.DoubleType, false),
            StructField("time", DataTypes.LongType, false)
        ))
        val userDF = spark.createDataFrame(ratingRDD, schema)
        userDF.createOrReplaceTempView("rating")
        println(">>>>>>==========rating表信息============================")
//        spark.sql("select * from rating limit 2").show()
    }
    /*
    2、movies.dat		数据格式为： 2::Jumanji (1995)::Adventure|Children's|Fantasy
        对应字段为：MovieID BigInt, Title String, Genres String
        对应字段中文解释：电影ID，电影名字，电影类型
     */
    def createMovies(spark:SparkSession): Unit = {
        val sc = spark.sparkContext
        val lines = sc.textFile("data/test/movies.dat")
        val movieRowRDD = lines.map(line => {
            val fields = line.split("::")
            val movieId = fields(0).toInt
            val movieName = fields(1)
            val movieType = fields(2)
            Row(movieId, movieName, movieType)
        })
        val schema = StructType(List(
            StructField("movie_id", DataTypes.IntegerType, false),
            StructField("movie_name", DataTypes.StringType, false),
            StructField("movie_type", DataTypes.StringType, false)
        ))

        val movieDF = spark.createDataFrame(movieRowRDD, schema)
        movieDF.createOrReplaceTempView("movie")
        println(">>>>>>==========movie表信息============================")
//        spark.sql("select * from movie limit 2").show()
    }
    def createUsers(spark:SparkSession): Unit = {
        val sc = spark.sparkContext
        val userLines = sc.textFile("data/test/users.dat")
        //2::M::56::16::70072
        //用户id，性别，年龄，职业，邮政编码
        val userRow = userLines.map(line => {
            val fields = line.split("::")
            val userId = fields(0).toInt
            val gender = fields(1)
            val age = fields(2).toInt
            val zip_code = fields(3).toInt
            Row(userId, gender, age, zip_code)
        })

        val schema = StructType(List(
            StructField("user_id", DataTypes.IntegerType, false),
            StructField("gender", DataTypes.StringType, false),
            StructField("age", DataTypes.IntegerType, false),
            StructField("zip_code", DataTypes.IntegerType, false)
        ))
        val userDF = spark.createDataFrame(userRow, schema)
        userDF.createOrReplaceTempView("user")
        println(">>>>>>==========users表信息============================")
//        spark.sql("select * from user limit 2").show()
    }




}
