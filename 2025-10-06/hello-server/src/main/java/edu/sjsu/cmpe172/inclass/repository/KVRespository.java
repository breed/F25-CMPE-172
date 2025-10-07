package edu.sjsu.cmpe172.inclass.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

public interface KVRespository extends JpaRepository<KV, String> {
}
