# Spring Code Review Style Guide

이 프로젝트는 **Spring Boot 3.4 / Java 21** 백엔드이며, **도메인 중심 Layered Architecture**를 채택한다.
아래 규칙을 기준으로 코드 리뷰를 수행한다.

---

## Review Comment Style & Language (전역 규칙)

- **"왜 문제인지"** + **"어떻게 고치면 좋은지"** 를 함께 제시한다.
- severity가 낮은 사항(nit)은 접두어 `[nit]`을 붙여 구분한다.
- 모든 코드 리뷰 코멘트는 **한국어로 작성한다.**
- 설명 또는 기술 용어가 영어가 자연스러울 경우 한국어 + 영어 병기로 작성할 수 있다.

---

## 1) 아키텍처 원칙

### 의존성 방향

```
Controller → Service → Repository → Entity
```

- **단방향 의존만 허용한다.** 하위 계층이 상위 계층을 참조하면 안 된다.
- `Repository`는 `Controller`를 알면 안 된다.
- `Service`는 `Controller`에 의존하면 안 된다.
- `Entity`는 `Controller`, `Service`를 알면 안 된다.

### 패키지 구조 (Feature-Based)

```
com.project
├─ domain/{feature}/
│   ├─ controller/            # REST 엔드포인트, ApiResponse<T> 반환
│   ├─ service/               # Interface + Impl 구조
│   │ 
│   ├─ repository/            # Spring Data JPA 인터페이스
│   ├─ entity/                # JPA 엔티티 (@Entity, @Builder)
│   ├─ dto/
│   │   ├─ request/           # 요청 DTO (Java record)
│   │   └─ response/          # 응답 DTO (Java record + static from())
│   └─ infra/                 # 인프라 어댑터
│       ├─ cache/             # Redis 캐시 Repository
│       └─ messaging/         # EventPublisher(Interface) + Kafka Producer/Consumer
└─ global/
    ├─ config/                # 횡단 설정 (Redis, Kafka, Swagger, CORS, ThreadPool)
    ├─ exception/             # ExceptionAdvice, BaseException, ErrorCode
    ├─ entity/                # BaseEntity (공통 엔티티)
    ├─ api/response/          # ApiResponse<T> 공통 응답 래퍼
    ├─ event/dto/             # 공유 Kafka 이벤트 Payload DTO
    └─ util/                  # RedisKeyGenerator
```

- 기술 기준이 아닌 **업무(Feature) 기준**으로 패키지를 나눈다.
- `global`은 횡단 관심사만 둔다. 도메인 비즈니스 로직을 `global`에 두지 않는다.

### 도입하지 않는 것

- Aggregate 엄격 설계
- CQRS 분리
- 도메인 이벤트 복잡 설계
- 과도한 인터페이스 남발

---

## 2) Entity 규칙

- **Setter 사용 금지.** 상태 변경은 비즈니스 의미를 가진 메서드로 수행한다.

```java
// 좋은 예
family.changeName(newName);

// 나쁜 예
family.setName(newName);
```

- Entity 생성은 `@Builder` 또는 `static factory method`를 사용한다.
- Entity는 `@NoArgsConstructor(access = AccessLevel.PROTECTED)`로 보호한다.
- Entity는 자신의 상태 변경 책임을 가진다.

---

## 3) DTO 규칙

### Response DTO

- `Entity → Response DTO` 변환은 DTO 내부의 `static from(Entity)` 메서드를 사용한다.
- Controller에서 `XxxResponse.from(entity)` 형태로 호출한다.
- DTO에 비즈니스 로직을 두지 않는다.

```java
public record FamilyResponse(Long id, String name, Long createdById) {
    public static FamilyResponse from(Family family) {
        return new FamilyResponse(family.getId(), family.getName(), family.getCreatedById());
    }
}
```

### Request DTO

- Java `record`로 작성하여 불변성을 보장한다.
- `Request DTO → Entity` 변환은 **Service에서 Builder로** 수행한다.
- Request DTO에 `toEntity()`를 두지 않는다.

```java
Family family = Family.builder()
    .name(request.name())
    .createdById(request.createdById())
    .build();
```

---

## 4) Service 규칙

- Service는 **Interface + Impl** 구조를 유지한다 (`FamilyService` + `FamilyServiceImpl`).
- Service의 책임: 트랜잭션 경계, 비즈니스 오케스트레이션, 여러 Repository 조합.
- `@Transactional`은 **Service에만** 선언한다. Controller, Repository에 선언 금지.
- 조회 전용 메서드에는 `@Transactional(readOnly = true)`를 사용한다.
- Service는 **Entity(또는 도메인 모델)를 반환**하고, Controller에서 DTO로 변환한다.
- Service가 비대해지면(100줄 초과) 역할 분리를 검토한다.

### Service에서 금지

- HTTP 관련 로직
- Kafka 파싱 로직
- Response DTO 직접 반환

---

## 5) API 응답 규칙

- 모든 Controller 응답은 `ApiResponse<T>`로 감싸야 한다.

```java
// 생성
return ApiResponse.created(FamilyResponse.from(entity));

// 조회
return ApiResponse.success(FamilyResponse.from(entity));
```

- Controller는 Entity를 직접 반환하거나 파라미터로 받지 않는다. 반드시 **Request/Response DTO**를 사용한다.

---

## 6) 예외 처리

- 비즈니스 예외는 `ApplicationException`(extends `BaseException`)을 던진다.
- 도메인별 `ErrorCode` enum을 정의하여 `BaseErrorCode` 인터페이스를 구현한다.
- `RuntimeException`이나 `IllegalStateException`을 직접 던지지 않는다.
- 클라이언트 원인(4xx)과 서버 원인(5xx)을 명확히 구분한다.
- 새 에러 코드 추가 시 `HttpStatus`, `customCode` 문자열, `message`(한글) 세 필드를 모두 채운다.

```java
@Getter
@RequiredArgsConstructor
public enum FamilyErrorCode implements BaseErrorCode {
    FAMILY_NOT_FOUND(HttpStatus.NOT_FOUND, "FAMILY_001", "가족을 찾을 수 없습니다");

    private final HttpStatus httpStatus;
    private final String customCode;
    private final String message;
}
```

- `ExceptionAdvice`(`@RestControllerAdvice`)가 모든 예외를 `ApiResponse.fail()`로 변환한다.

---

## 7) 도메인 간 참조 원칙

- 다른 도메인의 Entity를 직접 참조하지 않는다.
- **도메인 간 참조는 ID 기반으로만** 한다.

```java
// 나쁜 예: Entity 직접 참조
family.addMember(Customer customer)

// 좋은 예: ID 기반 참조
family.addMember(customerId)
```

- Service에서 다른 도메인 Repository 호출은 가능하나, 순환 의존은 피한다.

---

## 8) Infra 원칙 (Kafka / Redis)

### 이벤트 (Kafka)

- Kafka Consumer와 서비스별 메시징 어댑터는 `domain/{feature}/infra/messaging/`에 위치한다.
- Consumer는 메시지를 수신한 뒤 공통 지원 객체를 통해 파싱하고, Service로 즉시 위임한다. Consumer에 비즈니스 로직을 두지 않는다.
- 공통 이벤트 계약 DTO와 Kafka 지원 타입은 `lib-kafka` 라이브러리(`com.dabom.messaging.kafka...`)를 사용한다.
- topic / eventType / consumer group / notification subType 문자열은 서비스 로컬 상수 대신 라이브러리 계약 상수를 사용한다.
- Notification subtype 매핑은 서비스에서 직접 `switch`로 계산하지 않고, `NotificationEventSupport`를 사용한다.
- 공통 Kafka 설정은 라이브러리 기본 설정을 우선 사용하고, 서비스 전용 설정이 필요하면 별도 이름의 Bean으로 확장한다.
- 서비스 전용 Kafka 설정 클래스는 `domain/{feature}/infra/messaging/config/`에 위치한다.

### 캐시 (Redis)

- Redis 캐시 어댑터는 `infra/cache/`에 위치한다.
- Redis Key 규칙은 `global/util/RedisKeyGenerator`에서 통합 관리한다.
- 조회 시 **Cache Look-Aside** 패턴을 기본으로 사용한다: 캐시 확인 → 미스 시 DB 조회 → 캐시 저장.

---

## 9) Repository 규칙

- Repository는 **영속성(DB 접근)만** 담당한다.
- Repository에 비즈니스 로직(조건 분기, 정책 판단, 상태 변경 판단)을 두지 않는다.
- 복잡한 조회 조합/전략은 Service 계층에서 수행한다.

---

## 10) BaseEntity 공통 관리 원칙 (global)

### 🎯 목적
- **일관성 유지**: 생성/수정/삭제 시간 일관성 유지.
- **자동화**: JPA Auditing을 통한 날짜 자동 관리.
- **확장성**: 소프트 삭제(Soft Delete) 기반 확장 가능 구조 확보.
- **중복 제거**: 모든 엔티티에서 중복되는 날짜 필드 제거.

### 📦 위치
```text
global
└─ entity
    └─ BaseEntity
```

### 규칙
- 모든 도메인 엔티티는 `BaseEntity`를 상속(`extends`)받아 생성한다.
- 개별 엔티티 내에 `created_at`, `updated_at` 필드를 직접 선언하지 않는다.
- 삭제 로직 구현 시 `BaseEntity`의 `softDelete()` 메서드를 활용한다.

---

## 11) 트랜잭션 & DB

- N+1 가능성이 있으면 근거와 함께 지적하고, `@EntityGraph` 또는 `fetch join` 해결 방안을 제시한다.
- 조회 전용 쿼리는 DTO Projection 직접 반환을 권장하여 영속성 컨텍스트 오버헤드를 줄인다.

---

## 12) 코드 스타일

- **포매팅**: Google Java Format (AOSP, 4-space 인덴트) — `./gradlew spotlessApply`로 자동 적용.
- **Import 순서**: `java` → `javax` → `jakarta` → `org` → `net` → `com` → 기타 → `lombok` (Spotless가 강제).
- **Checkstyle**: Naver Java 컨벤션 적용. `./gradlew checkstyleMain`으로 검증.
- 메서드/변수 이름은 역할이 드러나게 작성하고, 약어를 남발하지 않는다.
- 매직넘버/문자열은 반드시 상수로 추출한다.

---

## 13) 테스트

- 신규 비즈니스 로직은 최소 단위 테스트 1개 이상 작성한다.
- 경계값과 예외 케이스를 각각 1개 이상 포함한다.

---

## 14) 안티패턴 체크리스트

리뷰 시 아래 패턴이 발견되면 반드시 지적한다:

| 안티패턴 | 설명 |
|----------|------|
| **Setter 사용** | `setXxx()` 대신 의미 있는 비즈니스 메서드 사용 |
| **Entity 직접 노출** | Controller가 Entity를 반환하면 `from()`으로 DTO 변환 |
| **BaseEntity 미사용** | 날짜 필드 직접 구현 대신 `extends BaseEntity` 사용 |
| **DTO에 toEntity()** | Request DTO에 변환 로직을 두지 않고 Service에서 Builder로 생성 |
| **Controller/Repository에 @Transactional** | 트랜잭션은 Service에만 선언 |
| **도메인 간 Entity 직접 참조** | ID 기반 참조로 변경 |
| **Consumer에 비즈니스 로직** | Consumer는 파싱 후 Service로 위임만 |
| **global에 도메인 로직** | global은 횡단 관심사만 허용 |
| **Service 비대화** | 100줄 초과 시 역할 분리 검토 |
| **RuntimeException 직접 throw** | `ApplicationException` + 도메인별 `ErrorCode`를 사용 |