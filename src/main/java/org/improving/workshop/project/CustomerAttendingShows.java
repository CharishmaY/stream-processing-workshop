package org.improving.workshop.exercises.stateful;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.improving.workshop.samples.PurchaseEventTicket;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;
import static org.improving.workshop.Streams.SERDE_TICKET_JSON;

@Slf4j
public class CustomerAttendingShows {

   public static final JsonSerde<VenueAddress> SERDE_VENUE_ADDRESS_JSON =
           new JsonSerde<>(CustomerAttendingShows.VenueAddress.class);

    public static void main(String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {

        KTable<String, Customer> customerTable = builder
                .table(
                        TOPIC_DATA_DEMO_CUSTOMERS,
                        Materialized
                                .<String, Customer>as(persistentKeyValueStore("customers"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_CUSTOMER_JSON)
                );

        KTable<String, Event> eventsTable = builder
                .table(
                        TOPIC_DATA_DEMO_EVENTS,
                        Materialized
                                .<String, Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_EVENT_JSON)
                );


    var addressOfCustomerTable =  builder
                .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON))
                .filter((k,v) -> v.customerid() != null)
                .selectKey((key, value) -> value.customerid())
                .toTable(Named.as("customer-only-address"),
                        Materialized
                                .<String, Address>as(persistentKeyValueStore("customer-only-address"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ADDRESS_JSON));

        var addressOfVenueTable =  builder
                .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON))
                .filter((k,v) -> v.customerid() == null)
                .selectKey((key, value) -> value.customerid())
                .toTable(Named.as("venue-only-address"),
                        Materialized
                                .<String, Address>as(persistentKeyValueStore("venue-only-address"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ADDRESS_JSON));

        var venueAndAddressTable = builder
                .stream(TOPIC_DATA_DEMO_VENUES, Consumed.with(Serdes.String(), SERDE_VENUE_JSON))
                .selectKey((k,v) -> v.addressid())
                .join(addressOfVenueTable, (addressId, venue, address) -> new VenueAddress(venue, address))
                .selectKey((k,v) -> v.venue.id())
                .toTable(Named.as("venue-and-address"),
                        Materialized
                                .<String, VenueAddress>as(persistentKeyValueStore("venue-and-address"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_VENUE_ADDRESS_JSON));

        builder.stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .selectKey((k,v) -> v.customerid())
                .join(customerTable, (customerId, ticket, customer) -> new TicketCustomer(ticket, customer))
                .join(addressOfCustomerTable, (customerId, ticketCustomer, addressOfCustomer) -> new TicketCustomerAddress(ticketCustomer, addressOfCustomer))
                .selectKey((k, v) -> v.ticketCustomer.ticket.eventid())
                .join(eventsTable, (eventId, ticketCustomerAddress, event) -> new TicketCustomerAddressEvent(ticketCustomerAddress, event))
                .selectKey((k, v) -> v.event.venueid())
                .join(venueAndAddressTable, (venueId, ticketCustomerAddressEvent, venueAndAddress) -> new TicketCustomerAddressEventVenueAndAddress(ticketCustomerAddressEvent, venueAndAddress))
                .selectKey((k, v) -> v.ticketCustomerAddressEvent.ticketCustomerAddress.addressOfCustomer.state()+v.ticketCustomerAddressEvent.event.id())
                .filter((k,v) -> !v.ticketCustomerAddressEvent.ticketCustomerAddress.addressOfCustomer.state().equals(v.venueAddress.address.state()))
                .groupByKey()
                .aggregate(FinalCustomerDetails::new,
                        (key, oldvalue, newValue)->{

                            return new FinalCustomerDetails();
                        })
                .toStream();






        System.out.println("");

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class VenueAddress {
        private Venue venue;
        private Address address;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TicketCustomer {
        private Ticket ticket;
        private Customer customer;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TicketCustomerAddress {
        private TicketCustomer ticketCustomer;
        private Address addressOfCustomer;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TicketCustomerAddressEvent {
        private TicketCustomerAddress ticketCustomerAddress;
        private Event event;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TicketCustomerAddressEventVenueAndAddress {
        private TicketCustomerAddressEvent ticketCustomerAddressEvent;
        private VenueAddress venueAddress;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class FinalCustomerDetails {
        private List<Customer> customers;
    }
}