package com.ericsson.sparklab

import java.io.File

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.springframework.boot.SpringApplication

object MainLab extends App {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (System.getProperty("os.name").toLowerCase().indexOf("win") >= 0) {
        val hadoopUtils : String = new File("src/main/resources/hadoop").getAbsolutePath()

        System.setProperty("hadoop.home.dir", hadoopUtils)
    }

    SpringApplication.run(classOf[Config])
}