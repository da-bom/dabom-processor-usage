package com.project.domain.example.infra.cache.dto;

import com.project.domain.example.entity.Example;

public record ExampleCacheDto(Long exampleId, String exampleName, String exampleContent) {
    public static ExampleCacheDto from(Example example) {
        return new ExampleCacheDto(
                example.getExampleId(), example.getExampleName(), example.getExampleContent());
    }

    public Example toEntity() {
        return Example.builder()
                .exampleId(exampleId)
                .exampleName(exampleName)
                .exampleContent(exampleContent)
                .build();
    }
}
