package com.example.demo.service.bitfinex;

import com.example.demo.controller.ws.Message;
import com.example.demo.service.bitmex.BitmexMessageMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;

import java.util.List;

@RequiredArgsConstructor
@Slf4j
public class BitfinexWebSocketHandler implements WebSocketHandler {
    private static final String TRADES_REQUEST = "{\"event\": \"subscribe\", \"channel\": \"trades\", \"pair\": \"BTCUSD\"}";
    private static final String PRICE_REQUEST = "{\"event\": \"subscribe\", \"channel\": \"ticker\", \"pair\": \"BTCUSD\"}";

    private final FluxSink<Message<?>> sink;

    @Override
    public Mono<Void> handle(WebSocketSession s) {
        return Flux.just(TRADES_REQUEST, PRICE_REQUEST)
                .map(s::textMessage)
                .as(s::send)
                .thenMany(s.receive())
                .map(WebSocketMessage::getPayloadAsText)
                .filter(payload -> payload.contains("[") && payload.contains("]"))
                .publishOn(Schedulers.parallel())
                .map(payload -> {
                    Message<?>[] messages = BitfinexMessageMapper.bitfinexToMessage(payload);
                    log.info("Bitfinex message: {}", payload);
                    return messages;
                })
                .flatMap(Flux::fromArray)
                .doOnNext(sink::next)
                .then();
    }
}
