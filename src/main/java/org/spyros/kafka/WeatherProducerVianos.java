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
public class WeatherProducerVianos {

    private static final String TOPIC = "weather-topic";

    @Autowired
    private KafkaTemplate<String, WeatherJson> kafkaTemplate;

    public void processCsv() {
        try {
            System.out.println("Vianos");
            Reader reader = Files.newBufferedReader(Paths.get("/home/spyros/Downloads/m_vianos_hourly.csv"));
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

            List<CSVRecord> records = csvParser.getRecords();

            for (CSVRecord record : records) {
                WeatherJson weather = new WeatherJson();
                weather.setDateTime(parseDateTime(record.get("Date"), record.get("Time")));
                weather.setBarometricPressure(record.get("ΒΙΑΝΝΟΣ Barometric Pressure"));
                weather.setWindSpeed(record.get("ΒΙΑΝΝΟΣ WIND SPEED"));
                weather.setPyronometer(record.get("ΒΙΑΝΝΟΣ Pyranometer 0 - 2000 W/m²"));
                weather.setPrecipitation(record.get("ΒΙΑΝΝΟΣ Precipitation"));
                weather.setRelativeHumidity(record.get("ΒΙΑΝΝΟΣ Relative Humidity"));
                weather.setWindDirection(record.get("ΒΙΑΝΝΟΣ WIND DIRECTION"));
                weather.setTemperature(record.get("ΒΙΑΝΝΟΣ Temperature"));
                weather.setHourlyETo(record.get("ΒΙΑΝΝΟΣ Hourly ETo"));
                weather.setRainDuration(record.get("ΒΙΑΝΝΟΣ Rain Duration"));
                weather.setTownId("4");

                // Send Weather object to Kafka
                kafkaTemplate.send(TOPIC, "vianos" + weather.getDateTime().toString(), weather);
            }

            csvParser.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}