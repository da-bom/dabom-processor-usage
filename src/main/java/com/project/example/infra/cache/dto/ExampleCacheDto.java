package com.project.example.infra.cache.dto;

import com.project.example.core.Example;

public record ExampleCacheDto(Long exampleId, String exampleName, String exampleContent) {
    /** Domain → Cache DTO */
    public static ExampleCacheDto from(Example example) {
        return new ExampleCacheDto(
                example.getExampleId(), example.getExampleName(), example.getExampleContent());
    }

    /** Cache DTO → Domain */
    public Example toDomain() {
        return Example.withId(exampleId, exampleName, exampleContent);
    }
}
