package com.project.domain.example.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import com.project.global.util.BaseEntity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "EXAMPLE")
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
public class Example extends BaseEntity {

    private static final int MIN_NAME_LENGTH = 1;
    private static final int MAX_NAME_LENGTH = 100;
    private static final int MIN_CONTENT_LENGTH = 1;
    private static final int MAX_CONTENT_LENGTH = 1000;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long exampleId;

    private String exampleName;

    private String exampleContent;

    public static Example create(String exampleName, String exampleContent) {
        validateExampleName(exampleName);
        validateExampleContent(exampleContent);

        return Example.builder().exampleName(exampleName).exampleContent(exampleContent).build();
    }

    public void update(String exampleName, String exampleContent) {
        validateExampleName(exampleName);
        validateExampleContent(exampleContent);

        this.exampleName = exampleName;
        this.exampleContent = exampleContent;
    }

    public boolean isModifiable() {
        return this.exampleId != null;
    }

    private static void validateExampleName(String exampleName) {
        if (exampleName == null || exampleName.isBlank()) {
            throw new IllegalArgumentException("Example name cannot be null or blank");
        }
        if (exampleName.length() < MIN_NAME_LENGTH || exampleName.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException(
                    String.format(
                            "Example name length must be between %d and %d",
                            MIN_NAME_LENGTH, MAX_NAME_LENGTH));
        }
    }

    private static void validateExampleContent(String exampleContent) {
        if (exampleContent == null || exampleContent.isBlank()) {
            throw new IllegalArgumentException("Example content cannot be null or blank");
        }
        if (exampleContent.length() < MIN_CONTENT_LENGTH
                || exampleContent.length() > MAX_CONTENT_LENGTH) {
            throw new IllegalArgumentException(
                    String.format(
                            "Example content length must be between %d and %d",
                            MIN_CONTENT_LENGTH, MAX_CONTENT_LENGTH));
        }
    }
}
