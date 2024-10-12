package org.spyros.kafka;

import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.springframework.stereotype.Component;
import org.spyros.model.Weather;
import org.spyros.model.WeatherJson;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.spyros.kafka.KafkaConsumer.parseDoubleOrDefault;

@Component
@NoArgsConstructor
public class WeatherJsonToWeatherMapper implements MapFunction<WeatherJson, Weather>, Serializable {

    @Override
    public Weather map(WeatherJson weatherJson) throws Exception {
        return Weather.builder()
                .dateTime(LocalDateTime.parse(weatherJson.getDateTime(), DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .windSpeed(parseDoubleOrDefault(weatherJson.getWindSpeed(), 0.0))
                .relativeHumidity(parseDoubleOrDefault(weatherJson.getRelativeHumidity(), 0.0))
                .temperature(parseDoubleOrDefault(weatherJson.getTemperature(), 0.0))
                .barometricPressure(parseDoubleOrDefault(weatherJson.getBarometricPressure(), 0.0))
                .pyronometer(parseDoubleOrDefault(weatherJson.getPyronometer(), 0.0))
                .precipitation(parseDoubleOrDefault(weatherJson.getPrecipitation(), 0.0))
                .windDirection(parseDoubleOrDefault(weatherJson.getWindDirection(), 0.0))
                .hourlyEto(parseDoubleOrDefault(weatherJson.getHourlyETo(), 0.0))
                .rainDuration(parseDoubleOrDefault(weatherJson.getRainDuration(), 0.0))
                .townId(Long.valueOf(weatherJson.getTownId()))
                .build();
    }
}
