package com.alura.pix.streams;

import com.alura.pix.dto.PixDTO;
import com.alura.pix.serdes.PixSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PixAggregator {

    @Autowired
    public void aggregator(StreamsBuilder streamsBuilder) {
        //KStream<String, PixDTO> messageStream = streamsBuilder
        //        .stream("pix-topic", Consumed.with(Serdes.String(), PixSerdes.serdes()))
        //        .peek((key, value) -> System.out.println("Pix recebido de: " + value.getChaveOrigem()))
        //        .filter((key, value) -> value.getValor() > 1000);
        // messageStream.print(Printed.toSysOut());
        // messageStream.to("pix-topic-verificação", Produced.with(Serdes.String(), PixSerdes.serdes()));
        KTable<String, Double> messageStream = streamsBuilder
                .stream("pix-topic", Consumed.with(Serdes.String(), PixSerdes.serdes()))
                .peek((key, value) -> System.out.println("Pix recebido de: " + value.getChaveOrigem()))
                .groupBy((key, value) -> value.getChaveOrigem())
                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> (aggregate + value.getValor()),
                        Materialized.with(Serdes.String(), Serdes.Double())
               );

        messageStream.toStream().print(Printed.toSysOut());
        messageStream.toStream().to("pix-topic-agregacao", Produced.with(Serdes.String(), Serdes.Double()));
    }
}
