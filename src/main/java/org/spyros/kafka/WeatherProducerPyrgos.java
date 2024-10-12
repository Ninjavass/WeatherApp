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
public class WeatherProducerPyrgos {

    private static final String TOPIC = "weather-topic";

    @Autowired
    private KafkaTemplate<String, WeatherJson> kafkaTemplate;

    public void processCsv() {
        try {
            System.out.println("Pyrgos");
            Reader reader = Files.newBufferedReader(Paths.get("/home/spyros/Downloads/m_pyrgos_hourly.csv"));
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

            List<CSVRecord> records = csvParser.getRecords();

            for (CSVRecord record : records) {
                WeatherJson weather = new WeatherJson();
                weather.setDateTime(parseDateTime(record.get("Date"), record.get("Time")));
                weather.setWindDirection(record.get("ΠΥΡΓΟΣ WIND DΙRECTION"));
                weather.setRelativeHumidity(record.get("ΠΥΡΓΟΣ Humidity"));
                weather.setBarometricPressure(record.get("ΠΥΡΓΟΣ BAROMETER"));
                weather.setPyronometer(record.get("ΠΥΡΓΟΣ SOLAR"));
                weather.setWindSpeed(record.get("ΠΥΡΓΟΣ WIND SPEED"));
                weather.setTemperature(record.get("ΠΥΡΓΟΣ AIR TEMP"));
                weather.setPrecipitation(record.get("ΠΥΡΓΟΣ Rain"));
                weather.setRainDuration(record.get("ΠΥΡΓΟΣ Rain Duration"));
                weather.setTownId("2");

                // Send Weather object to Kafka
                kafkaTemplate.send(TOPIC, "pyrgos" + weather.getDateTime().toString(), weather);
            }

            csvParser.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}