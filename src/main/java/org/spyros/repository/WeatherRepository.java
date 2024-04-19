package org.spyros.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.spyros.model.Weather;

import java.time.LocalDateTime;

@Repository
public interface WeatherRepository extends JpaRepository<Weather, LocalDateTime> {

}
