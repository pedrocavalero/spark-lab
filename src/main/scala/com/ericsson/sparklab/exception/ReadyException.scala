package com.ericsson.sparklab.exception

import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.http.HttpStatus

@ResponseStatus(value = HttpStatus.METHOD_FAILURE, reason = "Everything ready")
case class ReadyException(message: String = "Everything ready") extends Exception(message)