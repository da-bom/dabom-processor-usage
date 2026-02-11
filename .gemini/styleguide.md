# Spring Code Review Style Guide

이 프로젝트는 **Spring Boot 3.4 / Java 21** 백엔드이며, **Pragmatic Hexagonal Architecture**를 채택한다.
아래 규칙을 기준으로 코드 리뷰를 수행한다.

---

## 1) 아키텍처 원칙 (Pragmatic Hexagonal)

### 의존성 방향

```
Web(Controller) → Application(Service) → Core(Domain) ← Infra(JPA/Redis/Kafka)
```

- **Domain(core)이 중심이다.** 모든 의존성은 Domain을 향한다.
- Domain 패키지(`core/`)는 Spring, JPA, Kafka 등 **프레임워크 어노테이션을 사용하지 않는다.** 순수 Java(POJO)만 허용한다.
- `infra/` → `core/` 방향의 의존은 허용하지만, `core/` → `infra/` 방향의 의존은 **절대 금지**한다.
- `web/` 패키지가 `infra/` 패키지를 직접 참조하면 안 된다.

### 패키지 구조 (Feature-Based)

각 도메인은 아래 하위 패키지를 가진다.

```
com.project.{도메인}/
├── web/                  # Web Adapter (Controller + DTO + WebMapper)
│   ├── dto/request/
│   ├── dto/response/
│   └── {도메인}WebMapper
├── application/          # Application Layer (Service + Repository 인터페이스)
│   ├── {도메인}Service
│   └── repository/       # Command/Query Repository 인터페이스
├── core/                 # Domain Layer (Entity + Rule/Policy)
│   ├── {도메인}          # Aggregate Root (순수 POJO)
│   ├── {도메인}Rule      # 도메인 검증/계산 로직
│   └── event/            # 도메인 이벤트 DTO
└── infra/                # Infra Adapter (JPA Entity + EntityMapper + Repository 구현체)
    ├── entity/
    ├── mapper/
    ├── repository/
    ├── cache/
    └── messaging/
```

- 기술 기준(Controller, Service, Repository)이 아닌 **업무(Feature) 기준**으로 패키지를 나눈다.
- 여러 도메인이 공통으로 쓰는 로직은 `domain.common`에 둔다. **Util 클래스를 만들지 않는다.**

---

## 2) Domain 규칙 (핵심)

### Entity는 스스로 결정한다

- **Setter 사용 금지.** 상태 변경은 반드시 비즈니스 의미를 가진 메서드(예: `tryUpgradeLevel()`, `block()`)로만 수행한다.
- 비즈니스 규칙(if 조건, 검증)은 **Entity 또는 Rule/Policy 클래스 안**에 작성한다. Service에 작성하지 않는다.
- Domain Entity는 `@Entity`, `@Table` 같은 JPA 어노테이션 없이 **순수 POJO**로 작성한다. DB 매핑은 `infra/entity/`의 JpaEntity가 담당한다.
- Entity 생성은 `static factory method` 또는 `@Builder`를 사용한다.

### Rule / Policy 패턴

- **Rule(Policy)**: 조건 판단·검증만 수행한다. 상태를 변경하지 않는다.
- **Entity**: 상태 변경 책임을 가진다.
- **Service**: 흐름(조율)만 담당한다. `찾는다 → 시킨다 → 저장한다` 패턴을 따른다.

```
// 좋은 예: Service는 위임만 한다
user.tryUpgradeLevel();

// 나쁜 예: Service가 직접 판단하고 변경한다
if (user.getOrderCount() >= 50) { user.setLevel("VIP"); }
```

---

## 3) CQRS (Repository 분리)

- **CommandRepository**: 쓰기 전용. Domain Entity를 파라미터/반환값으로 사용한다. (`save`, `delete`, `findById`)
- **QueryRepository**: 읽기 전용. **DTO를 직접 반환**한다. Entity를 반환하지 않는다.
- Repository **인터페이스**는 `application/repository/`에, **구현체**는 `infra/repository/`에 위치한다.
- QueryRepository 구현 시 `Projections.constructor()`를 사용하여 DB → DTO 직접 매핑을 권장한다. (JPA Projection)
- 하나의 Repository에 `save`와 복잡한 조회 쿼리를 섞지 않는다.

---

## 4) Mapper (변환기)

프로젝트에는 두 종류의 Mapper가 존재한다. **Mapper에 비즈니스 로직을 넣지 않는다.**

| Mapper | 위치 | 역할 |
|--------|------|------|
| **WebMapper** | `web/` | Request DTO ↔ Domain, Domain ↔ Response DTO |
| **EntityMapper** | `infra/mapper/` | Domain ↔ JPA Entity |

- Mapper는 `static` 메서드로 작성한다.
- Entity가 Controller(web)로 직접 노출되면 안 된다. 반드시 Mapper를 거쳐 DTO로 변환한다.

---

## 5) API 응답 규칙

- 모든 Controller 응답은 `ApiResponse<T>`로 감싸야 한다 (`ApiResponse.success()`, `ApiResponse.created()`, `ApiResponse.fail()`).
- Controller는 Entity를 직접 반환하거나 파라미터로 받지 않는다. 반드시 **Request/Response DTO**를 사용한다.
- DTO는 Java `record`로 작성하여 불변성을 보장한다.
- 성공 응답은 `CommonSuccessCode`, 실패 응답은 도메인별 `ErrorCode` enum을 사용한다.

---

## 6) 예외 처리

- 비즈니스 예외는 `BaseException`을 상속하고 도메인별 `ErrorCode` enum(`BaseErrorCode` 구현)을 정의하여 던진다.
- `RuntimeException`이나 `IllegalStateException`을 직접 던지지 않는다.
- 클라이언트 원인(4xx)과 서버 원인(5xx)을 명확히 구분한다.
- 새 에러 코드 추가 시 `HttpStatus`, `code` 문자열, `message`(한글) 세 필드를 모두 채운다.

---

## 7) Service 규칙

- Service는 **흐름 제어(orchestration)만** 담당한다: 도메인 객체 로딩 → 도메인에게 위임 → 저장/이벤트 발행.
- Service에 `if`문으로 비즈니스 판단 로직을 작성하면 안 된다. Domain 또는 Rule로 이동시킨다.
- Service가 비대해지면(100줄 초과) 역할 분리를 검토한다.
- 조회 전용 메서드에는 `@Transactional(readOnly = true)`를 사용한다.
- Service는 DTO를 알지 않는다. 순수 Domain 객체만 사용한다. DTO 변환은 Controller(WebMapper)가 담당한다.

---

## 8) 이벤트 / Kafka

- 도메인 간 통신은 **Kafka 이벤트**를 통해 수행한다. 직접 서비스 호출을 하지 않는다.
- 이벤트 발행 인터페이스(`EventPublisher`)는 `core/event/`에, 구현체(`KafkaProducer`)는 `infra/messaging/`에 위치한다.
- 이벤트 Payload DTO는 `core/event/dto/`에 위치한다.

---

## 9) 캐시 (Redis)

- Redis 캐시 어댑터는 `infra/cache/`에 위치한다.
- Redis Key 규칙은 `global/redis/key/RedisKeyGenerator`에서 통합 관리한다.
- 조회 시 **Cache Look-Aside** 패턴을 기본으로 사용한다: 캐시 확인 → 미스 시 DB 조회 → 캐시 저장.

---

## 10) 트랜잭션 & DB

- N+1 가능성이 있으면 근거와 함께 지적하고, `@EntityGraph` 또는 `fetch join` 해결 방안을 제시한다.
- QueryDSL 사용 시 `BooleanExpression`을 조합하여 동적 쿼리를 구성한다.
- 조회 전용 쿼리는 `QueryRepository`에서 DTO Projection으로 직접 반환하여 영속성 컨텍스트 오버헤드를 줄인다.

---

## 11) 코드 스타일

- **포매팅**: Google Java Format (AOSP, 4-space 인덴트) — `./gradlew spotlessApply`로 자동 적용.
- **Import 순서**: `java` → `javax` → `jakarta` → `org` → `net` → `com` → 기타 → `lombok` (Spotless가 강제).
- **Checkstyle**: Naver Java 컨벤션 (Java 21 / Jakarta 수정 버전) 적용. `./gradlew checkstyleMain`으로 검증.
- 메서드/변수 이름은 역할이 드러나게 작성하고, 약어를 남발하지 않는다.
- 매직넘버/문자열은 반드시 상수로 추출한다.

---

## 12) 테스트

- 신규 비즈니스 로직은 최소 단위 테스트 1개 이상 작성한다.
- **Domain(core) 로직은 순수 Java이므로 Spring 컨텍스트 없이 단위 테스트가 가능해야 한다.**
- 경계값과 예외 케이스를 각각 1개 이상 포함한다.
- 테스트는 H2 인메모리 DB 위에서 실행된다 (`application-local.yaml`).

---

## 13) 리뷰 코멘트 스타일

- **"왜 문제인지"** + **"어떻게 고치면 좋은지"** 를 함께 제시한다.
- 한글로 작성한다.
- severity가 낮은 사항(nit)은 접두어 `[nit]`을 붙여 구분한다.

---

## 14) 안티패턴 체크리스트

리뷰 시 아래 패턴이 발견되면 반드시 지적한다:

| 안티패턴 | 설명 |
|----------|------|
| **Service에 비즈니스 로직** | `if` 조건 판단이 Service에 있으면 Domain/Rule로 이동 |
| **Setter 사용** | `setXxx()` 대신 의미 있는 비즈니스 메서드 사용 |
| **Entity 직접 노출** | Controller가 Entity를 반환하면 DTO + Mapper로 변환 |
| **core/ 패키지에 프레임워크 의존** | `@Entity`, `@Component` 등이 core에 있으면 infra로 이동 |
| **Util 클래스 생성** | `XxxUtil` 대신 Domain Service 또는 `domain.common`에 배치 |
| **하나의 Repository 혼용** | 쓰기/읽기가 섞여 있으면 Command/Query로 분리 |
| **Mapper에 로직** | Mapper에 조건분기/계산이 있으면 Domain으로 이동 |
| **Service 비대화** | 100줄 초과 시 역할 분리 검토 |
