package org.spyros.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.spyros.model.Town;

@Repository
public interface TownRepository extends JpaRepository<Town, Integer> {

    @Query(value = "SELECT * FROM town WHERE name = ?1", nativeQuery = true)
    Town findByName(String name);
}
