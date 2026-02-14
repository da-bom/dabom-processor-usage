package com.project.domain.customer.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.project.domain.customer.entity.Customer;

public interface CustomerRepository extends JpaRepository<Customer, Long> {
    Customer findByPhoneNumber(String phoneNumber);
}
