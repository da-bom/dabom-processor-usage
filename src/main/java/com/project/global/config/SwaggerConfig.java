package com.project.global.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;

@Configuration
public class SwaggerConfig {

    @Value("${project.name:Backend API}")
    private String projectName;

    @Value("${project.version:1.0.0}")
    private String projectVersion;

    @Bean
    public OpenAPI openApi() {
        return new OpenAPI()
                .info(
                        new Info()
                                .title(projectName)
                                .description(projectName + " Documentation")
                                .version(projectVersion));
    }
}
