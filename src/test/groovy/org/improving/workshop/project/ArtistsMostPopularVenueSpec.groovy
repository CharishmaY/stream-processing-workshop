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

class ArtistsMostPopularVenueSpec extends Specification {
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
        ArtistsMostPopularVenue.configureTopology(streamsBuilder)

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
                ArtistsMostPopularVenue.OUTPUT_TOPIC,
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
        venueInputTopic.pipeInput(venueId1, new Venue(venueId1, "address-1", "us-bank-stadium", 50))

        and: 'another venue for music events'
        String venueId2 = "venue-2"
        venueInputTopic.pipeInput(venueId2, new Venue(venueId2, "address-2", "target-center", 40))

        and: 'an event for artist-1 at venue-1'
        String eventId1 = "event-1"
        eventInputTopic.pipeInput(eventId1, new Event(eventId1, "artist-1", "venue-1", 10, "03/21/2024"))

        and: 'another event for artist-1 at venue-1'
        String eventId2 = "event-2"
        eventInputTopic.pipeInput(eventId2, new Event(eventId2, "artist-1", "venue-1", 20, "03/22/2024"))

        and: 'an event for artist-2 at venue-1'
        String eventId3 = "event-3"
        eventInputTopic.pipeInput(eventId3, new Event(eventId3, "artist-2", "venue-1", 10, "03/23/2024"))

        and: 'another event for artist-2 at venue-1'
        String eventId4 = "event-4"
        eventInputTopic.pipeInput(eventId4, new Event(eventId4, "artist-2", "venue-1", 20, "03/24/2024"))

        and: 'an event for artist-2 at venue-2'
        String eventId5 = "event-5"
        eventInputTopic.pipeInput(eventId5, new Event(eventId5, "artist-2", "venue-2", 20, "03/25/2024"))

        and: 'an event for artist-1 at venue-2'
        String eventId6 = "event-6"
        eventInputTopic.pipeInput(eventId6, new Event(eventId6, "artist-1", "venue-2", 20, "03/26/2024"))

        and: 'another event for artist-1 at venue-2'
        String eventId7 = "event-7"
        eventInputTopic.pipeInput(eventId7, new Event(eventId7, "artist-1", "venue-2", 20, "03/27/2024"))

        and: 'purchased tickets for the events'
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-2", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-3", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-4", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-5", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-6", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-7", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-8", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-9", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-10", eventId1))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-11", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-12", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-13", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-14", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-15", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-16", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-17", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-18", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-19", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-20", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-21", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-22", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-23", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-24", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-25", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId3))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-2", eventId3))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-3", eventId3))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-4", eventId3))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-5", eventId3))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-6", eventId3))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-7", eventId3))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-8", eventId3))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-9", eventId3))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-10", eventId3))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-11", eventId4))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-12", eventId4))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-13", eventId4))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-14", eventId4))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-15", eventId4))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-16", eventId4))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-17", eventId4))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-18", eventId4))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-19", eventId4))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-20", eventId4))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-2", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-3", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-4", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-5", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-6", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-7", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-8", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-9", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-10", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-11", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-12", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-13", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-14", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-15", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-16", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-17", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-18", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-19", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-20", eventId5))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-2", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-3", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-4", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-5", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-6", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-7", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-8", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-9", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-10", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-11", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-12", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-13", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-14", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-15", eventId6))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-2", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-3", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-4", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-5", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-6", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-7", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-8", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-9", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-10", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-11", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-12", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-13", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-14", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-15", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-16", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-17", eventId7))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-18", eventId7))

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        outputRecords.size() == 2

        // record is for artist 1
        outputRecords[0].key() == "artist-1"
        outputRecords[0].value() == "us-bank-stadium"

        // record is for artist 2
        outputRecords[1].key() == "artist-2"
        outputRecords[1].value() == "target-center"
    }
}
