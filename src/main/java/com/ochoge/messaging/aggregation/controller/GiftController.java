package com.ochoge.messaging.aggregation.controller;

import com.ochoge.messaging.aggregation.domain.Gift;
import com.ochoge.messaging.aggregation.domain.ReciprocalGift;
import com.ochoge.messaging.aggregation.service.GiftService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/gifts")
@RequiredArgsConstructor
public class GiftController {
    private final GiftService giftService;

    @PostMapping
    public ResponseEntity<List<ReciprocalGift>> sendAndReceive(
            @RequestBody Gift gift) {
        List<ReciprocalGift> replies = giftService.sendAndAwait(gift);
        return ResponseEntity.ok(replies);
    }
}
