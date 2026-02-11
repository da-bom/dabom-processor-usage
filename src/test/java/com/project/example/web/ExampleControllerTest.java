package com.project.example.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.example.application.ExampleService;
import com.project.example.core.Example;
import com.project.example.web.dto.request.CreateExampleRequest;
import com.project.example.web.dto.request.UpdateExampleRequest;

@WebMvcTest(ExampleController.class)
class ExampleControllerTest {

    @Autowired private MockMvc mockMvc;

    @Autowired private ObjectMapper objectMapper;

    @MockitoBean private ExampleService exampleService;

    @MockitoBean private ExampleWebMapper exampleWebMapper;

    @Test
    @WithMockUser
    @DisplayName("GET /example/{id} - Example 조회 성공")
    void findByIdReturnsOk() throws Exception {
        // given
        Long exampleId = 1L;
        Example example = Example.withId(exampleId, "test", "content");
        given(exampleService.findById(exampleId)).willReturn(example);

        // when & then
        mockMvc.perform(get("/example/{exampleId}", exampleId))
                .andDo(print())
                .andExpect(status().isOk());

        verify(exampleService).findById(exampleId);
    }

    @Test
    @WithMockUser
    @DisplayName("POST /example - Example 생성 성공")
    void createReturnsOk() throws Exception {
        // given
        CreateExampleRequest request = new CreateExampleRequest("name", "content");
        Example example = Example.create("name", "content");
        Example createdExample = Example.withId(1L, "name", "content");
        given(exampleWebMapper.toDomain(any(CreateExampleRequest.class))).willReturn(example);
        given(exampleService.create(any(Example.class))).willReturn(createdExample);

        // when & then
        mockMvc.perform(
                        post("/example")
                                .with(SecurityMockMvcRequestPostProcessors.csrf())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                .andDo(print())
                .andExpect(status().isOk());

        verify(exampleService).create(any(Example.class));
    }

    @Test
    @WithMockUser
    @DisplayName("PUT /example/{id} - Example 업데이트 성공")
    void updateReturnsOk() throws Exception {
        // given
        Long exampleId = 1L;
        UpdateExampleRequest request = new UpdateExampleRequest("updated", "updated content");
        Example example = Example.withId(exampleId, "updated", "updated content");
        given(exampleWebMapper.toDomain(any(Long.class), any(UpdateExampleRequest.class)))
                .willReturn(example);
        given(exampleService.update(any(Example.class))).willReturn(example);

        // when & then
        mockMvc.perform(
                        put("/example/{exampleId}", exampleId)
                                .with(SecurityMockMvcRequestPostProcessors.csrf())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                .andDo(print())
                .andExpect(status().isOk());

        verify(exampleService).update(any(Example.class));
    }
}
