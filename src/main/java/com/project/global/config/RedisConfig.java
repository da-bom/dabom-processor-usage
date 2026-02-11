package com.project.global.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.project.example.infra.cache.dto.ExampleCacheDto;

@Configuration
public class RedisConfig {

    @Bean
    public RedisTemplate<String, ExampleCacheDto> exampleCacheRedisTemplate(
            RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, ExampleCacheDto> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        // key: String
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());

        // value: JSON (record 지원)
        GenericJackson2JsonRedisSerializer valueSerializer =
                new GenericJackson2JsonRedisSerializer();

        template.setValueSerializer(valueSerializer);
        template.setHashValueSerializer(valueSerializer);

        template.afterPropertiesSet();
        return template;
    }
}
