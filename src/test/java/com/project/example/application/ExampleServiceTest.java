package com.project.example.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.project.example.application.repository.ExampleCommandRepository;
import com.project.example.application.repository.ExampleQueryRepository;
import com.project.example.core.Example;
import com.project.example.core.event.ExampleEventPublisher;
import com.project.example.infra.cache.ExampleCacheRepository;

@ExtendWith(MockitoExtension.class)
class ExampleServiceTest {

    @Mock private ExampleCommandRepository exampleCommandRepository;

    @Mock private ExampleQueryRepository exampleQueryRepository;

    @Mock private ExampleEventPublisher exampleEventPublisher;

    @Mock private ExampleCacheRepository exampleCacheRepository;

    @InjectMocks private ExampleService exampleService;

    @Test
    @DisplayName("findById - Example 도메인 객체 조회 성공 (Cache Hit)")
    void findByIdReturnsExampleFromCache() {
        // given
        Long exampleId = 1L;
        Example expected = Example.withId(exampleId, "test", "content");
        given(exampleCacheRepository.findById(exampleId)).willReturn(Optional.of(expected));

        // when
        Example result = exampleService.findById(exampleId);

        // then
        assertThat(result).isEqualTo(expected);
        verify(exampleCacheRepository).findById(exampleId);
    }

    @Test
    @DisplayName("findById - Example 도메인 객체 조회 성공 (Cache Miss)")
    void findByIdReturnsExampleFromDb() {
        // given
        Long exampleId = 1L;
        Example expected = Example.withId(exampleId, "test", "content");
        given(exampleCacheRepository.findById(exampleId)).willReturn(Optional.empty());
        given(exampleQueryRepository.findById(exampleId)).willReturn(Optional.of(expected));

        // when
        Example result = exampleService.findById(exampleId);

        // then
        assertThat(result).isEqualTo(expected);
        verify(exampleCacheRepository).findById(exampleId);
        verify(exampleQueryRepository).findById(exampleId);
        verify(exampleCacheRepository).save(expected);
    }

    @Test
    @DisplayName("create - Example 도메인 객체 생성 성공 (DB + Event + Cache)")
    void createSavesExample() {
        // given
        Example example = Example.create("name", "content");
        Example savedExample = Example.withId(1L, "name", "content");
        given(exampleCommandRepository.save(any(Example.class))).willReturn(savedExample);

        // when
        Example result = exampleService.create(example);

        // then
        assertThat(result).isEqualTo(savedExample);
        verify(exampleCommandRepository).save(any(Example.class));
        verify(exampleEventPublisher).publishExampleCreated(savedExample);
        verify(exampleCacheRepository).save(savedExample);
    }

    @Test
    @DisplayName("update - Example 도메인 객체 업데이트 성공 (DB + Cache)")
    void updateSavesExample() {
        // given
        Long exampleId = 1L;
        Example existingExample = Example.withId(exampleId, "old", "old content");
        Example updatedExample = Example.withId(exampleId, "new", "new content");

        // findById mock for verification
        given(exampleCacheRepository.findById(exampleId)).willReturn(Optional.of(existingExample));
        given(exampleCommandRepository.save(any(Example.class))).willReturn(updatedExample);

        // when
        Example result = exampleService.update(updatedExample);

        // then
        assertThat(result).isEqualTo(updatedExample);
        verify(exampleCacheRepository).findById(exampleId);
        verify(exampleCommandRepository).save(any(Example.class));
        verify(exampleCacheRepository).save(updatedExample);
    }
}
