package org.improving.workshop.project

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.music.event.Event
import org.msse.demo.mockdata.music.ticket.Ticket
import org.msse.demo.mockdata.music.venue.Venue
import spock.lang.Specification

import static org.improving.workshop.utils.DataFaker.TICKETS

class ArtistsMostPopularEventSpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Event> eventInputTopic
    TestInputTopic<String, Ticket> ticketInputTopic
    TestInputTopic<String, Venue> venueInputTopic

    // outputs
    TestOutputTopic<String, String> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the ArtistsMostPopularVenue topology (by reference)
        ArtistsMostPopularEvent.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        )

        ticketInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        )

        venueInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_VENUES,
                Serdes.String().serializer(),
                Streams.SERDE_VENUE_JSON.serializer()
        )

        outputTopic = driver.createOutputTopic(
                ArtistsMostPopularEvent.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        )
    }

    def 'cleanup'() {
        driver.close()
    }

    def "artists most popular venue"() {
        given: 'a venue for music events'
        String venueId1 = "venue-1"
        venueInputTopic.pipeInput(venueId1, new Venue(venueId1, "address-1", "us-bank-stadium", 15))

        and: 'another venue for music events'
        String venueId2 = "venue-2"
        venueInputTopic.pipeInput(venueId2, new Venue(venueId2, "address-2", "target-center", 10))

        and: 'an event for artist-1 at venue-1'
        String eventId1 = "event-1"
        eventInputTopic.pipeInput(eventId1, new Event(eventId1, "artist-1", "venue-1", 5, "03/21/2024"))

        and: 'another event for artist-1 at venue-1'
        String eventId2 = "event-2"
        eventInputTopic.pipeInput(eventId2, new Event(eventId2, "artist-1", "venue-1", 5, "03/22/2024"))

        and: 'an event for artist-1 at venue-2'
        String eventId3 = "event-3"
        eventInputTopic.pipeInput(eventId3, new Event(eventId3, "artist-1", "venue-2", 5, "03/23/2024"))

        and: 'another event for artist-1 at venue-2'
        String eventId4 = "event-4"
        eventInputTopic.pipeInput(eventId4, new Event(eventId4, "artist-1", "venue-2", 5, "03/24/2024"))

        and: 'purchased tickets for the events'
        ticketInputTopic.pipeInput(TICKETS.generate("customer-5", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-6", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId3))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-2", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-4", eventId4))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-3", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-2", eventId2))

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        outputRecords.size() == 8
        println outputRecords.stream().map(r -> r.key() + '_' + r.value()).toList()
    }
}
