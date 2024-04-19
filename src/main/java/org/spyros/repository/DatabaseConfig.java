//package org.spyros.repository;
//
//import org.springframework.boot.CommandLineRunner;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.spyros.model.Town;
//
//import java.util.List;
//
//@Configuration
//public class DatabaseConfig {
//    @Bean
//    CommandLineRunner commandLineRunner(TownRepository townRepository) {
//        return args -> {
//            Town town1 = new Town("Doxaro");
//            townRepository.saveAll(List.of(town1));
//        };
//    }
//}
//
//
