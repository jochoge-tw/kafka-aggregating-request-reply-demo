package com.ochoge.messaging.aggregation.messaging;

import com.ochoge.messaging.aggregation.messaging.publisher.CustomHeaderAggregatingReplyReleaseStrategy;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CustomHeaders {
    /**
     * Used with the {@link CustomHeaderAggregatingReplyReleaseStrategy} to specify the size in inbound replies.
     */
    public static final String EXPECTED_REPLIES_COUNT = "x-expected-replies-count";
}
