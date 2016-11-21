package com.ericsson.sparklab.controller

import java.io.File

import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.beans.factory.annotation.Autowired

import com.ericsson.sparklab.service.MovieLensService

@RestController
@RequestMapping(Array("ninja"))
class NinjaController @Autowired()(private val movieLensService: MovieLensService) {

    @RequestMapping(Array("/hi"))
    def hi(): String = {
        "scala cabulox"
    }

    @RequestMapping(Array("trigger"))
    def trigger() = {
        this.movieLensService.learn()

        "OK"
    }

}