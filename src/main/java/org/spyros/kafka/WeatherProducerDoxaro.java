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
public class WeatherProducerDoxaro {

    private static final String TOPIC = "weather-topic";

    @Autowired
    private KafkaTemplate<String, WeatherJson> kafkaTemplate;

    public void processCsv() {
        try {
            System.out.println("Doxaro");
            Reader reader = Files.newBufferedReader(Paths.get("/home/spyros/Downloads/m_doxaro_hourly.csv"));
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

            List<CSVRecord> records = csvParser.getRecords();

            for (CSVRecord record : records) {
                WeatherJson weather = new WeatherJson();
                weather.setDateTime(parseDateTime(record.get("Date"), record.get("Time")));
                weather.setWindSpeed(record.get("ΔΟΞΑΡΟ WIND SPEED"));
                weather.setRelativeHumidity(record.get("ΔΟΞΑΡΟ Relative Humidity"));
                weather.setTemperature(record.get("ΔΟΞΑΡΟ Temperature"));
                weather.setBarometricPressure(record.get("ΔΟΞΑΡΟ Barometric Pressure"));
                weather.setPyronometer(record.get("ΔΟΞΑΡΟ Pyranometer 0 - 2000 W/m²"));
                weather.setPrecipitation(record.get("ΔΟΞΑΡΟ Precipitation"));
                weather.setWindDirection(record.get("ΔΟΞΑΡΟ WIND DIRECTION"));
                weather.setHourlyETo(record.get("ΔΟΞΑΡΟ Hourly ETo"));
                weather.setRainDuration(record.get("ΔΟΞΑΡΟ Rain Duration"));
                weather.setTownId("1");

                // Send Weather object to Kafka
                kafkaTemplate.send(TOPIC, "doxaro" + weather.getDateTime().toString(), weather);
            }

            csvParser.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}