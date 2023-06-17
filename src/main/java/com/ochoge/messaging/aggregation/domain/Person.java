package com.ochoge.messaging.aggregation.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person {
    private String name;

    public static Person named(String name) {
        return new Person(name);
    }
}
