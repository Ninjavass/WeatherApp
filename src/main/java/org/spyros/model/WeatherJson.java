package org.spyros.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@NoArgsConstructor
public class WeatherJson {
    @JsonProperty("dateTime")
    private String dateTime;

    @JsonProperty("windSpeed")
    private String windSpeed;

    @JsonProperty("relativeHumidity")
    private String relativeHumidity;

    @JsonProperty("temperature")
    private String temperature;

    @JsonProperty("barometricPressure")
    private String barometricPressure;

    @JsonProperty("pyronometer")
    private String pyronometer;

    @JsonProperty("precipitation")
    private String precipitation;

    @JsonProperty("windDirection")
    private String windDirection;

    @JsonProperty("hourlyETo")
    private String hourlyETo;

    @JsonProperty("rainDuration")
    private String rainDuration;

    @JsonProperty("townId")
    private String townId;
}
