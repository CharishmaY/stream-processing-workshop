package org.improving.workshop.project


import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.customer.address.Address
import org.msse.demo.mockdata.customer.profile.Customer
import org.msse.demo.mockdata.music.artist.Artist
import org.msse.demo.mockdata.music.event.Event
import org.msse.demo.mockdata.music.stream.Stream
import org.msse.demo.mockdata.music.ticket.Ticket
import org.msse.demo.mockdata.music.venue.Venue
import spock.lang.Specification

class CustomerStreamingArtistSpec extends Specification{

    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Address> addressInputTopic
    TestInputTopic<String, Customer> customerInputTopic
    TestInputTopic<String, Stream> streamInputTopic
    TestInputTopic<String, Artist> artistInputTopic

    TestOutputTopic<String, String> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the AddressSortAndStringify topology (by reference)
        CustomerStreamingArtist.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())


        customerInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_CUSTOMERS,
                Serdes.String().serializer(),
                Streams.SERDE_CUSTOMER_JSON.serializer()
        )

        streamInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_STREAMS,
                Serdes.String().serializer(),
                Streams.SERDE_STREAM_JSON.serializer()
        )

        artistInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ARTISTS,
                Serdes.String().serializer(),
                Streams.SERDE_ARTIST_JSON.serializer()
        )


        addressInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ADDRESSES,
                Serdes.String().serializer(),
                Streams.SERDE_ADDRESS_JSON.serializer()
        )

        outputTopic = driver.createOutputTopic(
                CustomerStreamingArtist.OUTPUT_TOPIC_ARTIST,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        )
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "artists with customers in same city listening to them"() {

        given: 'a set of customers'
        def cust1 = new Customer("cust123", "PREMIUM", "M", "John", "Steven", "James", "JSJ", "", "", "1989-01-20", "2022-01-02")
        def cust2 = new Customer("cust456", "PREMIUM", "M", "Jane", "Jo", "James", "JJJ", "", "", "1990-01-20", "2022-01-02")
        def cust3 = new Customer("cust567", "PREMIUM", "M", "George", "Mo", "James", "GMJ", "", "", "1999-01-20", "2022-01-02")
        def cust4 = new Customer("cust890", "PREMIUM", "M", "Jane", "Jo", "James", "JJJ", "", "", "1990-01-20", "2022-01-02")
        def cust5 = new Customer("cust111", "PREMIUM", "M", "George", "Mo", "James", "GMJ", "", "", "1999-01-20", "2022-01-02")

        and: 'address of these customers'
        def address1 = new Address("addr123", "cust123", "cd", "HOME", "111 1st St", "Apt 2", "Madison", "WI", "55555", "1233", "USA", 0L, 0L)
        def address2 = new Address("addr123", "cust456", "cd", "HOME", "111 1st St", "Apt 2", "Madison", "WI", "55555", "1233", "USA", 0L, 0L)
        def address3 = new Address("addr567", "cust567", "cd", "HOME", "111 21st St", "Apt 3", "Madison", "WI", "55554", "1235", "USA", 0L, 0L)
        def address4 = new Address("addr890", "cust777", "cd", "HOME", "111 1st St", "Apt 2", "Madison", "CA", "55555", "1233", "USA", 0L, 0L)
        def address5 = new Address("addr111", "cust222", "cd", "HOME", "111 21st St", "Apt 3", "Madison", "OH", "55554", "1235", "USA", 0L, 0L)

        and: 'artist'
        def artist1 = new Artist("artist123","michale","pop")
        def artist2 = new Artist("artist456","jack","pop")
        def artist3 = new Artist("artist789","son","rock")
        def artist4 = new Artist("artist890","barry","pop")
        def artist5 = new Artist("artist111","sorry","sock")

        and: 'stream'
        def stream1 = new Stream("stream123", "cust123","artist123","4")
        def stream2 = new Stream("stream456", "cust456","artist123","4")
        def stream3 = new Stream("stream567", "cust567","artist123","4")
        def stream4 = new Stream("stream890", "cust654","artist777","6")
        def stream5 = new Stream("stream111", "cust432","artist666","5")

        when: 'piping inputs through the stream'
        customerInputTopic.pipeInput(cust1.id(), cust1)
        customerInputTopic.pipeInput(cust2.id(), cust2)
        customerInputTopic.pipeInput(cust3.id(), cust3)

        addressInputTopic.pipeInput(address1.id(), address1)
        addressInputTopic.pipeInput(address2.id(), address2)
        addressInputTopic.pipeInput(address3.id(), address3)

        artistInputTopic.pipeInput(artist1.id(), artist1)
        artistInputTopic.pipeInput(artist2.id(), artist2)
        artistInputTopic.pipeInput(artist3.id(), artist3)

        streamInputTopic.pipeInput(stream1.id(), stream1)
        streamInputTopic.pipeInput(stream2.id(), stream2)
        streamInputTopic.pipeInput(stream3.id(), stream3)

        and: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        outputRecords.size() == 3

    }

}