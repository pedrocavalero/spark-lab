package com.ericsson.sparklab.controller

import java.io.File

import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.http.MediaType


import com.ericsson.sparklab.service.MovieLensService

@RestController
@RequestMapping(Array("ninja"))
class NinjaController @Autowired()(private val movieLensService: MovieLensService) {

    @RequestMapping(Array("/hi"))
    def hi(): String = {
      "scala cabulox"
    }

    @RequestMapping(Array("start"))
    def trigger() = {
        this.movieLensService.start()

        "OK"
    }
    
    @RequestMapping(value = Array("recommendations"), produces = Array(MediaType.TEXT_PLAIN_VALUE))
    def rec(@RequestParam("id") idUser: Int) = {
        val movieList = this.movieLensService.recommendation(idUser)
        val output = StringBuilder.newBuilder
            
        var i = 1
        output.append("[\n")
        movieList.foreach { r =>
            output.append("{ movieId: %s, rating: %s }\n".format(r.product, r.rating))

            i += 1
        }
        output.append("\n]")
        output
    }

    @RequestMapping(value = Array("ratings"), produces = Array(MediaType.TEXT_PLAIN_VALUE))
    def ratings(@RequestParam("id") idUser: Int) = {
        val r = this.movieLensService.getRatingsByUserId(idUser)
        val output = StringBuilder.newBuilder
          
        output.append("[\n")
        var i = 1
        r.foreach { r =>
            if(i!=1){
              output.append(",")
            }
            output.append("{ movieId: %s, rating: %s }\n".format(r.product, r.rating))
            i += 1
        }
        output.append("\n]")
        output
    }
    
    
}