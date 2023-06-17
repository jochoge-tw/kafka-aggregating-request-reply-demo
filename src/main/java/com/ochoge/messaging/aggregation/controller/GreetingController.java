package com.ochoge.messaging.aggregation.controller;

import com.ochoge.messaging.aggregation.domain.Greeting;
import com.ochoge.messaging.aggregation.domain.GreetingReply;
import com.ochoge.messaging.aggregation.service.GreetingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/greetings")
@RequiredArgsConstructor
public class GreetingController {
    private final GreetingService greetingService;

    @PostMapping
    public ResponseEntity<List<GreetingReply>> sendAndReceive(
            @RequestBody Greeting greeting) {
        List<GreetingReply> replies = greetingService.sendAndAwait(greeting);
        return ResponseEntity.ok(replies);
    }
}
