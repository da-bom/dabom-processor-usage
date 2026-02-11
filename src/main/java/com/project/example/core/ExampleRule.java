package com.project.example.core;

/** Example 도메인 규칙 (Policy) - 유효성 검증(Validation) 및 비즈니스 제약 사항 관리 - 도메인 로직에서 호출하여 사용 */
public class ExampleRule {

    private static final int MIN_NAME_LENGTH = 1;
    private static final int MAX_NAME_LENGTH = 100;
    private static final int MIN_CONTENT_LENGTH = 1;
    private static final int MAX_CONTENT_LENGTH = 1000;

    /**
     * Example 이름 유효성 검증
     *
     * @param exampleName 검증할 이름
     * @throws IllegalArgumentException 유효하지 않은 이름인 경우
     */
    public static void validateExampleName(String exampleName) {
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

    /**
     * Example 내용 유효성 검증
     *
     * @param exampleContent 검증할 내용
     * @throws IllegalArgumentException 유효하지 않은 내용인 경우
     */
    public static void validateExampleContent(String exampleContent) {
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

    /**
     * Example이 수정 가능한 상태인지 확인
     *
     * @param example 확인할 Example 객체
     * @return 수정 가능 여부
     */
    public static boolean isModifiable(Example example) {
        // 비즈니스 규칙: ID가 있는 경우만 수정 가능
        return example.getExampleId() != null;
    }
}
