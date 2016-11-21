package com.ericsson.sparklab.exception

import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.http.HttpStatus

@ResponseStatus(value = HttpStatus.METHOD_FAILURE, reason = "Processing...")
case class ProcessingException(message: String = "Processing...") extends Exception(message)