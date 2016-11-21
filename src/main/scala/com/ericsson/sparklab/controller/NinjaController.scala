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
    def hi(): String = "scala cabulox"

    @RequestMapping(Array("start"))
    def trigger() = {
        this.movieLensService.start()

        "OK"
    }
    
    @RequestMapping(value = Array("rec"), produces = Array(MediaType.TEXT_PLAIN_VALUE))
    def rec(@RequestParam("id") idUser: Int) = {
        val movieList = this.movieLensService.recommendation(idUser)
        val output = StringBuilder.newBuilder
            
        var i = 1
        
        output.append("Movies recommended for you (%d):\n".format(idUser))
        movieList.foreach { r =>
            output.append("%2d: %s\n".format(i, r))

            i += 1
        }

        output
    }

}