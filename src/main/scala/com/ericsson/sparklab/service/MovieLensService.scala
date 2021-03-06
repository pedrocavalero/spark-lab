package com.ericsson.sparklab.service

import java.io.File

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ ALS, Rating, MatrixFactorizationModel }

import org.springframework.stereotype.Service
import org.springframework.scheduling.annotation.Async

import com.ericsson.sparklab.exception.NotReadyException
import com.ericsson.sparklab.exception.ProcessingException
import com.ericsson.sparklab.exception.ReadyException

import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.ListBuffer
import java.util.Date


@Service
class MovieLensService {
    
    var sc : SparkContext = null
    var ready = false
    var processing = false
    var test : RDD[Rating] = null
    var bestModel: Option[MatrixFactorizationModel] = None

    var myRatings : Seq[Rating] = null
    var ratings : RDD[(Long, Rating)] = null
    var movies : Map[Int, String] = null
    var myRatingsRDD: RDD[Rating] = null
    
    var validationRmse: Double = 0
    var imdb: JValue = null
    var imdbList: List[Movie] = null
    
    @Async
    def start(rank: Int, lambda: Double, numIter: Int) {
        if (this.processing) {
            throw new ProcessingException
        }
        if (!this.ready) {
          //throw new ReadyException
          //destroy();
          val conf = new SparkConf()
        		  .setAppName("MovieLensALS")
        		  .set("spark.executor.memory", "2g")
        		  .setMaster("local[2]")
        		  
        	this.sc = new SparkContext(conf)
          sc.setCheckpointDir("'checkpoint/")
        }
        
        this.processing = true
        

        this.train(rank: Int, lambda: Double, numIter: Int)

        this.ready = true
        this.processing = false
    }
    
    def train(rank: Int, lambda: Double, numIter: Int) {
        // load data
        if(!ready)
          loadData()

        // split ratings into train (60%), validation (20%), and test (20%) based on the 
        // last digit of the timestamp, add myRatings to train, and cache them

        val numPartitions = 4
        val training = ratings.filter(x => x._1 < 6)
            .values
            .union(myRatingsRDD)
            .repartition(numPartitions)
            .cache()
        val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
            .values
            .repartition(numPartitions)
            .cache()
        this.test = ratings.filter(x => x._1 >= 8).values.cache()

        val numTraining = training.count()
        val numValidation = validation.count()
        val numTest = test.count()

        println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

        // train models and evaluate them on the validation set

        //chooseBestModel(training, validation)
        runModel(training, validation, rank, lambda, numIter)//8, 0.1, 20)
        
    }
    
    def runModel(training: RDD[Rating], validation: RDD[Rating], rank: Int, lambda: Double, numIter: Int){
      val numValidation = validation.count()
      val model = ALS.train(training, rank, numIter, lambda)
      this.validationRmse = computeRmse(model, validation, numValidation)     
      bestModel = Some(model)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
                + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
    }
    
    def chooseBestModel(training: RDD[Rating], validation: RDD[Rating]){
        val numValidation = validation.count()

        val ranks = List(8, 12)
        val lambdas = List(0.1, 10.0)
        val numIters = List(10, 20)
        var bestValidationRmse = Double.MaxValue
        var bestRank = 0
        var bestLambda = -1.0
        var bestNumIter = -1
        for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
            val model = ALS.train(training, rank, numIter, lambda)
            this.validationRmse = computeRmse(model, validation, numValidation)
            
            println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
                + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
                
            if (validationRmse < bestValidationRmse) {
                bestModel = Some(model)
                bestValidationRmse = validationRmse
                bestRank = rank
                bestLambda = lambda
                bestNumIter = numIter
            }
        }
    }
    
    def loadData(){
              // load ratings and movie titles
        val movieLensHomeDir = "./" 

        this.myRatingsRDD = sc.textFile(new File(movieLensHomeDir, "personalRatings.txt").toString).map { line =>
            val fields = line.split("::")
            Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
        }.filter(_.rating > 0.0)
        
     		this.myRatings = myRatingsRDD.collect().toSeq
        
        this.ratings = sc.textFile(new File(movieLensHomeDir, "ratings.dat").toString).map { line =>
        		val fields = line.split("::")
        		// format: (timestamp % 10, Rating(userId, movieId, rating))
        		(fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
        }
        
        this.movies = sc.textFile(new File(movieLensHomeDir, "movies.dat").toString).map { line =>
          val fields = line.split("::")
          // format: (movieId, movieName)
          (fields(0).toInt, fields(1))
        }.collect().toMap
        
        var auxList = new ListBuffer[Movie]()
        this.imdb = parse(file2JsonInput(new File(movieLensHomeDir, "imdb.json")))
        implicit val formats = DefaultFormats
        imdb.children.foreach { 
          movie => {
            val mOpt = movie.extractOpt[Movie]
            mOpt foreach { m => auxList +=m }
          }
        }
        this.imdbList = auxList.toList
    }
    

    
    def statistics() = {
      this.validationRmse
    }
    
    def recommendation(idUser : Int): Seq[Rating] = {
        if (!this.ready) {
            throw new NotReadyException
        }
        
        val myRatedMovieIds = myRatings.map(_.product).toSet
        val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
        val recommendations = bestModel.get
            .predict(candidates.map((idUser, _)))
            .collect()
            .sortBy(-_.rating)
            .take(50)

        recommendations
    }
    
    def destroy() {
        sc.stop()
    }

    def getImdbById(id:String):Movie={
      val list = imdbList filter( _.id == id)
      if(list.size>0)
        list(0)
      else
        null
    }
    
    def setRating(userId:Int, movieId: Int, rating: Double){
      val r = Rating(userId,movieId,rating)
      println("Adicionando o rating " + r + " no RDD")
      if(userId==0){
    	  val rRDD = this.sc.makeRDD(Seq(r))
 			  this.myRatingsRDD = this.myRatingsRDD.union(rRDD)
 			  this.myRatings = myRatingsRDD.collect().toSeq
      } else {
        val rRDD = this.sc.makeRDD(Seq((new Date().getTime(),r)))
        this.ratings = this.ratings.union(rRDD)
      }
    }

    /** Compute RMSE (Root Mean Squared Error). */
    def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
        val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
        val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
            .join(data.map(x => ((x.user, x.product), x.rating)))
            .values
        math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
    }

    /** Load ratings from file. */
    def loadRatings(path: String): Seq[Rating] = {
        val lines = Source.fromFile(path).getLines()
        val ratings = lines.map { line =>
            val fields = line.split("::")
            Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
        }.filter(_.rating > 0.0)
        if (ratings.isEmpty) {
            sys.error("No ratings provided.")
        } else {
            ratings.toSeq
        }
    }
    
    def getRatingsByUserId(userId: Int): Seq[Rating] = {
        if(userId==0){
        	myRatings.filter( r => r.user == userId)
        } else {
        	ratings.filter( r => r._2.user == userId).map(r => r._2).collect()
        }
          
    }
}

case class Movie(id: String, raw: MovieRaw, metadata: MovieMetadata)
case class MovieRaw(id: String, name: String, year: Int, genre: List[String])
case class MovieMetadata(
    Title: String, 
    Year: String, 
    Rated: String,
    Released: String,
    Runtime: String,
    Genre: String,
    Director: String,
    Writer: String,
    Actors: String,
    Plot: String,
    Language: String,
    Country: String,
    Awards: String,
    Poster: String,
    Metascore: String,
    imdbRating: String,
    imdbVotes: String,
    imdbID: String,
    Type: String,
    Response: String
)