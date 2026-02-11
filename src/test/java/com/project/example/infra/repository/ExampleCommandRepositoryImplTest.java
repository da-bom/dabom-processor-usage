package com.project.example.infra.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.project.example.core.Example;
import com.project.example.infra.entity.ExampleJpaEntity;
import com.project.example.infra.mapper.ExampleEntityMapper;

@ExtendWith(MockitoExtension.class)
class ExampleCommandRepositoryImplTest {

    @Mock private JpaExampleRepository jpaExampleRepository;

    @Mock private ExampleEntityMapper exampleEntityMapper;

    @InjectMocks private ExampleCommandRepositoryImpl exampleCommandRepository;

    @Test
    @DisplayName("save - Example 도메인 객체 저장 성공")
    void saveSavesExample() {
        // given
        Example example = Example.create("test", "content");
        ExampleJpaEntity entity =
                ExampleJpaEntity.builder().exampleName("test").exampleContent("content").build();
        ExampleJpaEntity savedEntity =
                ExampleJpaEntity.builder()
                        .exampleId(1L)
                        .exampleName("test")
                        .exampleContent("content")
                        .build();
        Example savedExample = Example.withId(1L, "test", "content");

        given(exampleEntityMapper.toEntity(example)).willReturn(entity);
        given(jpaExampleRepository.save(entity)).willReturn(savedEntity);
        given(exampleEntityMapper.toDomain(savedEntity)).willReturn(savedExample);

        // when
        Example result = exampleCommandRepository.save(example);

        // then
        assertThat(result).isEqualTo(savedExample);
        verify(jpaExampleRepository).save(any(ExampleJpaEntity.class));
    }
}
