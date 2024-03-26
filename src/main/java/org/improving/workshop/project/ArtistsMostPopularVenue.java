package org.improving.workshop.project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.*;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Goals -
 * 1. Identify the most popular venue for each artist
 */
public class ArtistsMostPopularVenue {

    private static final Logger log = LoggerFactory.getLogger(ArtistsMostPopularVenue.class);
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-artists-most-popular-venue";

    public static final JsonSerde<EventVenue> SERDE_EVENT_VENUE_JSON = new JsonSerde<>(EventVenue.class);
    public static final JsonSerde<EventVenueTicket> SERDE_EVENT_VENUE_TICKET_JSON = new JsonSerde<>(EventVenueTicket.class);
    public static final JsonSerde<SortedCounterMap> POPULARITY_SCP_JSON_SERDE = new JsonSerde<>(SortedCounterMap.class);
    public static final JsonSerde<LinkedHashMap<String, Float>> LINKED_HASH_MAP_JSON_SERDE = new JsonSerde<>(LinkedHashMap.class);

    /**
     * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {

        KTable<String, Venue> venuesTable = builder
                .table(
                        TOPIC_DATA_DEMO_VENUES,
                        Materialized
                                .<String, Venue>as(persistentKeyValueStore("venues"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_VENUE_JSON)
                );

        venuesTable.toStream().peek((key, venue) -> log.info("Venue '{}' available with name '{}' with a capacity of {}.", key, venue.name(), venue.maxcapacity()));

        KTable<String, Long> ticketsCountPerEvent = builder
                .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .peek((ticketId, ticketRequest) -> log.info("Ticket Requested: {}", ticketRequest))
                .groupBy((key, value) -> value.eventid())
                .count();
                //.toStream()
               /*.toTable(Named.as("events-tickets-count"),
                        Materialized
                                .<String, Long>as(persistentKeyValueStore("events-tickets-count"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                );*/

        ticketsCountPerEvent.toStream().peek((key, count) -> log.info("Event '{}' has '{}' tickets sold out.", key, count));

        builder
                .stream(TOPIC_DATA_DEMO_EVENTS, Consumed.with(Serdes.String(), SERDE_EVENT_JSON))
                .peek((eventId, event) -> System.out.println("Event Processed:" + eventId))
                .selectKey((key, value) -> value.venueid())
                //.toTable()
                .join(venuesTable,
                        (venueId, event, venue) -> new EventVenue(event, venue),
                        Joined.with(Serdes.String(), SERDE_EVENT_JSON, SERDE_VENUE_JSON)
                )
                .selectKey((key, value) -> value.getEvent().id())
                .peek((k,v) -> log.info("Key = {} , Value = {}",k,v))
                .toTable()
                .join(ticketsCountPerEvent,
                        (eventVenue, ticket_count) -> new EventVenueTicket(eventVenue,ticket_count),
                        Materialized.with(Serdes.String(),SERDE_EVENT_VENUE_TICKET_JSON)
                        //Joined.with(Serdes.String(), SERDE_EVENT_VENUE_JSON, Serdes.Long())
                )
                .toStream()
                .groupBy((key,value) -> value.getEvent().artistid())
                .aggregate(
                        // Initializer
                        SortedCounterMap::new,

                        // aggregator
                        (artistId, eventVenueTicket, venuePopularity) -> {

                            //float EvPopularity = venuePopularity.CalculateEventPopularity();

                            venuePopularity.calculateVenuePopularity();

                            return venuePopularity;
                        },
                        Materialized
                                .<String, SortedCounterMap>as(persistentKeyValueStore("venue-popularity-table"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(POPULARITY_SCP_JSON_SERDE)
                )
                .toStream()
                .mapValues((k,v) -> v.top(1))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), LINKED_HASH_MAP_JSON_SERDE));
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventVenue {
        private Event event;
        private Venue venue;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventTicket {
        private String eventId;
        private String ticketId;
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventVenueTicket {
        private Event event;
        private Venue venue;
        private Long ticket_count;

        public EventVenueTicket(EventVenue eventVenue, Long ticket_count)
        {
             this.event = eventVenue.event;
             this.venue = eventVenue.venue;
             this.ticket_count = ticket_count;
        }
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SortedCounterMap {
        private long ticketsSold;
        private float EvPopularity;
        private int totalEvents = 0;
        private EventVenueTicket eventVenueTicket;
        private int eventCapacity;
        private String eventId;
        private LinkedHashMap<String, Float> map;

        public SortedCounterMap(EventVenueTicket eventVenueTicket)
        {
            this.ticketsSold = eventVenueTicket.ticket_count;
            this.map = new LinkedHashMap<>();
            this.eventId = getEventVenueTicket().getEvent().id();
            this.eventCapacity = getEventVenueTicket().getEvent().capacity();
        }

        /*public float CalculateEventPopularity() {

            return (float) ticketsSold / eventCapacity;
        }*/

        public void calculateVenuePopularity() {

            //map.compute(artistId, (k, v) -> v == null ? 1 : v + 1);

            map.put(getEventVenueTicket().getEvent().id(), (float) ticketsSold / eventCapacity);
            totalEvents++;

            //this.map = map.entrySet().stream()
                    //.sorted(reverseOrder(Map.Entry.comparingByValue()))

                    //.collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        public LinkedHashMap<String, Float> top(int limit) {
            return map.entrySet().stream()
                    .sorted(reverseOrder(Map.Entry.comparingByValue()))
                    .limit(limit)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }
    }
}