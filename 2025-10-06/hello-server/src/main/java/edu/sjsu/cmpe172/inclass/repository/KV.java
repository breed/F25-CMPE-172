package edu.sjsu.cmpe172.inclass.repository;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class KV {
    @Id
    public String kvKey;
    public String kvValue;
}
