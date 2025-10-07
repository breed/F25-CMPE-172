package edu.sjsu.cmpe172.inclass.controller;

import edu.sjsu.cmpe172.inclass.repository.KV;
import edu.sjsu.cmpe172.inclass.repository.KVRespository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ExampleController {
    private final KVRespository kvRepository;

    @Value("token.seed")
    String tokenSeed;

    public ExampleController(KVRespository kvRespository) {
        this.kvRepository = kvRespository;
        var kv = new KV();
        kv.kvKey = "key" + System.currentTimeMillis();
        kv.kvValue = "value" + System.currentTimeMillis();
        kvRespository.save(kv);
    }
    @GetMapping("/")
    public String getSlash() {
        return "Hello world...";
    }

    @GetMapping("/kv")
    public Page<KV> getKv(Pageable pageable) {
        return kvRepository.findAll(pageable);
    }
}
