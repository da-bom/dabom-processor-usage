package com.project.domain.example.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.project.domain.example.entity.Example;

public interface ExampleRepository extends JpaRepository<Example, Long> {}
