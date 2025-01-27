package me.kafkaparallelconsumer.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

class CustomObjectMapper : ObjectMapper() {
    init {
        registerKotlinModule()
    }
}
