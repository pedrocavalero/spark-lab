package com.ericsson.sparklab.controller

import java.io.File

import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.http.MediaType


import com.ericsson.sparklab.service.MovieLensService
import java.lang.Double

import org.json4s.jackson.Serialization._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.mllib.recommendation.Rating

@RestController
@RequestMapping(Array("recflix"))
class RecflixController @Autowired()(private val movieLensService: MovieLensService) {

    @RequestMapping(Array("/hi"))
    def hi(): String = {
      "Recflix Rulez"
    }
    @RequestMapping(Array("/statistics"))
    def statistics(): Double = {
      this.movieLensService.validationRmse
    }
    @RequestMapping(Array("start"))
    def start(@RequestParam(value="rank", required = false) rank: Integer, 
        @RequestParam(value="lambda", required = false) lambda: Double, 
        @RequestParam(value="numIter", required = false) numIter: Integer) = {
      
        if(rank==null||lambda==null||numIter==null){
          this.movieLensService.start(8, 0.1, 20)        
        } else {
          this.movieLensService.start(rank, lambda, numIter)
        }

        "OK"
    }

    @RequestMapping(value = Array("recommendations"), produces = Array(MediaType.TEXT_PLAIN_VALUE))
    def rec(@RequestParam("id") idUser: Int) = {
      val movieList = this.movieLensService.recommendation(idUser)
		  mergeMovies(movieList)
    }

    def mergeMovies(movieList:Seq[Rating]):String ={
 		  implicit val formats = DefaultFormats
      val output = StringBuilder.newBuilder
      output.append("[\n")
      var i = 1
      movieList.foreach { r =>
        val movie = this.movieLensService.getImdbById(r.product.toString())
        if(movie!=null){
        	if(i!=1){
        		output.append(",")
        	}
     			output.append("{ \"id\":\"%s\", \"title\":\"%s\",\"year\":\"%s\",\"poster\":\"%s\",\"plot\":\"%s\",\"rating\":\"%s\"} ".
     			    format(r.product, movie.metadata.Title, movie.metadata.Year, movie.metadata.Poster, movie.metadata.Plot, r.rating))
     			output.append("\n")
        }
        i+=1
      }
      output.append("\n]")
      output.toString()
    }
    @RequestMapping(value = Array("ratings"), produces = Array(MediaType.TEXT_PLAIN_VALUE))
    def ratings(@RequestParam("id") idUser: Int) = {
        val r = this.movieLensService.getRatingsByUserId(idUser)
        mergeMovies(r)
    }
    
    
}