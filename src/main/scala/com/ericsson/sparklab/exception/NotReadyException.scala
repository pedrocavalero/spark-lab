package com.ericsson.sparklab.exception

import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.http.HttpStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST, reason = "Not ready yet")
case class NotReadyException(message: String = "Not ready yet") extends Exception(message)