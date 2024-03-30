package org.improving.workshop.project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.improving.workshop.Streams;
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
public class ArtistsMostPopularEvent {

    private static final Logger log = LoggerFactory.getLogger(ArtistsMostPopularEvent.class);
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-artists-most-popular-event";

    public static final JsonSerde<EventVenue> SERDE_EVENT_VENUE_JSON = new JsonSerde<>(EventVenue.class);
    public static final JsonSerde<EventVenueTicket> SERDE_EVENT_VENUE_TICKET_JSON = new JsonSerde<>(EventVenueTicket.class);
    public static final JsonSerde<SortedCounterMap> POPULARITY_SCP_JSON_SERDE = new JsonSerde<>(SortedCounterMap.class);
    public static final JsonSerde<LinkedHashMap<String, Float>> LINKED_HASH_MAP_JSON_SERDE = new JsonSerde<>(LinkedHashMap.class);

    static {
        // this may not have been needed once other things were done, but leaving it here - Neil
        Map<String, Object> config = new HashMap<>();
        config.put("spring.json.trusted.packages", "*");
        SERDE_EVENT_VENUE_JSON.configure(config, false);
        SERDE_EVENT_VENUE_TICKET_JSON.configure(config, false);
        Streams.SERDE_EVENT_JSON.configure(config, false);
    }
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

        ticketsCountPerEvent.toStream().peek((key, count) -> log.info("Event '{}' has '{}' tickets sold out.", key, count));

        Materialized<String, EventVenueTicket, KeyValueStore<Bytes, byte[]>> store = Materialized.<String, EventVenueTicket, KeyValueStore<Bytes, byte[]>>as("foo")
                .withKeySerde(Serdes.String())
                .withValueSerde(SERDE_EVENT_VENUE_TICKET_JSON);

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
                .toTable(Materialized.<String, EventVenue, KeyValueStore<Bytes, byte[]>>as("bar").withValueSerde(SERDE_EVENT_VENUE_JSON))
                .join(ticketsCountPerEvent,
                        (eventVenue, ticket_count) -> new EventVenueTicket(eventVenue,ticket_count),
                     store
                     //   Materialized.with(Serdes.String(), SERDE_EVENT_VENUE_TICKET_JSON)
                      //  Materialized.as("foo").withKeySerde(Serdes.String()).withValueSerde(SERDE_EVENT_VENUE_TICKET_JSON))
                        //Joined.with(Serdes.String(), SERDE_EVENT_VENUE_JSON, Serdes.Long())
                )
                .toStream()
                .filter((k, v) -> v.event != null && v.venue != null)
                .groupBy((key,value) -> value.getEvent().artistid(), Grouped.with(null, SERDE_EVENT_VENUE_TICKET_JSON))
                .aggregate(
                        // Initializer
                        () -> null,

                        // aggregator
                        (artistId, eventVenueTicket, eventPopularity) -> {

                            if (eventPopularity == null) {
                                eventPopularity = new SortedCounterMap(eventVenueTicket);
                            }
                            //float EvPopularity = eventPopularity.CalculateEventPopularity();

                            eventPopularity.calculateEventPopularity(eventVenueTicket);

                            return eventPopularity;
                        },
                        Materialized
                                .<String, SortedCounterMap>as(persistentKeyValueStore("event-popularity-table"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(POPULARITY_SCP_JSON_SERDE)
                )
                .toStream()
                .mapValues((k,v) -> v.top())
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


    @NoArgsConstructor
    @Data
    public static class Foo {
      long ticketCount;
      int capacity;

      public Foo(long ticketCount, int capacity) {
        this.ticketCount = ticketCount;
        this.capacity = capacity;
      }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SortedCounterMap {
        private long ticketsSold;
        private float EvPopularity;
        private int totalEvents = 0;
       // private EventVenueTicket eventVenueTicket;
        private int eventCapacity;
        private String eventId;
        private LinkedHashMap<String, Foo> map;

        public SortedCounterMap(EventVenueTicket eventVenueTicket)
        {
            this.ticketsSold = eventVenueTicket.ticket_count;
            this.map = new LinkedHashMap<>();
            this.eventId = eventVenueTicket.getEvent().id();
            this.eventCapacity = eventVenueTicket.getEvent().capacity();
           // this.eventVenueTicket = eventVenueTicket;
        }


        public void calculateEventPopularity(EventVenueTicket ticket) {

            map.compute(ticket.getEvent().id(), (k, v) -> (v == null) ?
                            new Foo(ticket.ticket_count, ticket.getEvent().capacity()) :
                            new Foo(v.ticketCount + ticket.ticket_count, ticket.getEvent().capacity()))
        ;}

        public LinkedHashMap<String, Float> top() {
            return map.entrySet().stream()
                    //.map(entry -> (float) entry.getValue().ticketCount / entry.getValue().capacity)
                    .map(entry -> Map.entry(entry.getKey(), (float) entry.getValue().ticketCount / entry.getValue().capacity))
                    //.sorted(reverseOrder(Map.Entry.comparingByValue()))
                    .sorted(reverseOrder(Map.Entry.comparingByValue()))
                    .limit(1)
                    .collect(toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e1, LinkedHashMap::new));
        }
    }
}