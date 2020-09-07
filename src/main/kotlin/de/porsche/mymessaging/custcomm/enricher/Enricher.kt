package de.porsche.mymessaging.custcomm.enricher

import de.porsche.mymessaging.custcomm.kafka.dto.Message
import javax.enterprise.context.ApplicationScoped

@ApplicationScoped
class Enricher {

    fun enrich(message: Message): Message {
        message.getDynamicContext()["enriched"] = "true"
        return message
    }
}