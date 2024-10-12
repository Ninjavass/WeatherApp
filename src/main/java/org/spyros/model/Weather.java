package org.spyros.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.time.LocalDateTime;

@Data
@Entity
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Weather {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private LocalDateTime dateTime;
    private Double windSpeed;
    private Double relativeHumidity;
    private Double temperature;
    private Double barometricPressure;
    private Double pyronometer;
    private Double precipitation;
    private Double windDirection;
    private Double hourlyEto;
    private Double rainDuration;
//    @ManyToOne
//    @JoinColumn(name = "town_id", foreignKey = @ForeignKey(name = "fk_town"))
//    private Town town;
    private Long townId;

}
