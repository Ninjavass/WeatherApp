package org.spyros.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.table.expressions.In;
import org.spyros.model.Town;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Entity
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Weather {

    @Id
    private LocalDateTime dateTime;
    private Double windSpeed;
    private Double relativeHumidity;
    private Double temperature;
    private Double barometricPressure;
    private Double pyronometer;
    private Double precipitation;
    private Double windDirection;
    private Double hourlyETo;
    private Double rainDuration;
    @ManyToOne
    @JoinColumn(name = "id", foreignKey = @ForeignKey(name = "fk_town"))
    private Town town;

}
