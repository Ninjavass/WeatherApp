package org.spyros.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.spyros.model.Weather;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface WeatherRepository extends JpaRepository<Weather, LocalDateTime> {
    @Query(value = "SELECT * FROM Weather LIMIT 20", nativeQuery = true)
    List<Weather> findAllLimitedToTwenty();

    @Query(value = "SELECT * FROM Weather WHERE town_id = :townId LIMIT 20", nativeQuery = true)
    List<Weather> findAllByTownLimitedToTwenty(@Param("townId") int townId);
}
