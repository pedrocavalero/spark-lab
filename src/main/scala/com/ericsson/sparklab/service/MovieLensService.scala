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
    
    @Async
    def start() {
        if (this.processing) {
            throw new ProcessingException
        }
        if (this.ready) {
            //throw new ReadyException
          destroy();
        }
        
        this.processing = true
        
        val conf = new SparkConf()
            .setAppName("MovieLensALS")
            .set("spark.executor.memory", "2g")
            .setMaster("local[2]")
            
        this.sc = new SparkContext(conf)

        this.train

        this.ready = true
        this.processing = false
    }
    
    def train() {
        // load data
        if(!ready)
          loadData()
        
        //myRatingsRDD = this.sc.parallelize(this.myRatings, 1)

//        val numRatings = ratings.count()
//        val numUsers = ratings.map(_._2.user).distinct().count()
//        val numMovies = ratings.map(_._2.product).distinct().count()
//
//        println("Got " + numRatings + " ratings from "
//            + numUsers + " users on " + numMovies + " movies.")

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
        runModel(training, validation, 8, 0.1, 20)
        
    }
    
    def runModel(training: RDD[Rating], validation: RDD[Rating], rank: Int, lambda: Double, numIter: Int){
      val numValidation = validation.count()
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)     
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
            val validationRmse = computeRmse(model, validation, numValidation)
            
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
    }
    
    def statistics() {
        // evaluate the best model on the test set
        val testRmse = computeRmse(bestModel.get, test, test.count())

       /* println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
            + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

        // create a naive baseline and compare it with the best model

        val meanRating = training.union(validation).map(_.rating).mean
        val baselineRmse =
            math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
        val improvement = (baselineRmse - testRmse) / baselineRmse * 100
        println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.") */
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
