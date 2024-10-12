package org.spyros.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.spyros.model.WeatherJson;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.spyros.kafka.KafkaUtils.parseDateTime;

@Service
@RequiredArgsConstructor
public class WeatherProducerRousoxwria {

    private static final String TOPIC = "weather-topic";

    @Autowired
    private KafkaTemplate<String, WeatherJson> kafkaTemplate;

    public void processCsv() {
        try {
            System.out.println("Rousoxwria");
            Reader reader = Files.newBufferedReader(Paths.get("/home/spyros/Downloads/m_rousoxwria_hourly.csv"));
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

            List<CSVRecord> records = csvParser.getRecords();

            for (CSVRecord record : records) {
                WeatherJson weather = new WeatherJson();
                weather.setDateTime(parseDateTime(record.get("Date"), record.get("Time")));
                weather.setPrecipitation(record.get("ΡΟΥΣΟΧΩΡΙΑ Precipitation"));
                weather.setBarometricPressure(record.get("ΡΟΥΣΟΧΩΡΙΑ Barometric Pressure"));
                weather.setWindSpeed(record.get("ΡΟΥΣΟΧΩΡΙΑ WIND SPEED"));
                weather.setWindDirection(record.get("ΡΟΥΣΟΧΩΡΙΑ WIND DIRECTION"));
                weather.setTemperature(record.get("ΡΟΥΣΟΧΩΡΙΑ Temperature"));
                weather.setRelativeHumidity(record.get("ΡΟΥΣΟΧΩΡΙΑ Relative Humidity"));
                weather.setPyronometer(record.get("ΡΟΥΣΟΧΩΡΙΑ Pyranometer 0 - 2000 W/m²"));
                weather.setRainDuration(record.get("ΡΟΥΣΟΧΩΡΙΑ Rain Duration"));
                weather.setHourlyETo(record.get("ΡΟΥΣΟΧΩΡΙΑ Hourly ETo"));
                weather.setTownId("3");

                // Send Weather object to Kafka
                kafkaTemplate.send(TOPIC, "rousoxwria" + weather.getDateTime().toString(), weather);
            }

            csvParser.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}