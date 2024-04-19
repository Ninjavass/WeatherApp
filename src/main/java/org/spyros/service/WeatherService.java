package org.spyros.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.springframework.stereotype.Service;
import org.spyros.model.Town;
import org.spyros.model.Weather;
import org.spyros.repository.TownRepository;
import org.spyros.repository.WeatherRepository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
@AllArgsConstructor
@Slf4j
public class WeatherService {
    private final WeatherRepository weatherRepository;
    private final TownRepository townRepository;

    private static final Map<String, Integer> columnToIndex = Map.of("wind_speed", 1, "relative_humidity", 2, "temperature", 3, "barometric_pressure", 4
            , "pyronometer", 5, "precipitation", 6, "wind_direction", 7, "hourlyeto", 8, "rain_duration", 9);

    public List<Weather> getAllData() {
        return weatherRepository.findAll();
    }

    public void mergedTables() {

        Map<String, String> doxaroMap;
        doxaroMap = new LinkedHashMap<>();
        doxaroMap.put("dateTime", "Date");
        doxaroMap.put("ΔΟΞΑΡΟ WIND SPEED", "wind_speed");
        doxaroMap.put("ΔΟΞΑΡΟ Relative Humidity", "relative_humidity");
        doxaroMap.put("ΔΟΞΑΡΟ Temperature", "temperature");
        doxaroMap.put("ΔΟΞΑΡΟ Barometric Pressure", "barometric_pressure");
        doxaroMap.put("ΔΟΞΑΡΟ Pyranometer 0 - 2000 W/m²", "pyronometer");
        doxaroMap.put("ΔΟΞΑΡΟ Precipitation", "precipitation");
        doxaroMap.put("ΔΟΞΑΡΟ WIND DIRECTION", "wind_direction");
        doxaroMap.put("ΔΟΞΑΡΟ Hourly ETo", "hourlyeto");
        doxaroMap.put("ΔΟΞΑΡΟ Rain Duration", "rain_duration");

        Map<String, String> pyrgosMap;
        pyrgosMap = new LinkedHashMap<>();
        pyrgosMap.put("dateTime", "Date");
        pyrgosMap.put("ΠΥΡΓΟΣ WIND DIRECTION", "wind_direction");
        pyrgosMap.put("ΠΥΡΓΟΣ Humidity", "relative_humidity");
        pyrgosMap.put("ΠΥΡΓΟΣ BAROMETER", "barometric_pressure");
        pyrgosMap.put("ΠΥΡΓΟΣ SOLAR", "pyronometer");
        pyrgosMap.put("ΠΥΡΓΟΣ WIND SPEED", "wind_speed");
        pyrgosMap.put("ΠΥΡΓΟΣ AIR TEMP", "temperature");
        pyrgosMap.put("ΠΥΡΓΟΣ Rain", "precipitation");
        pyrgosMap.put("ΠΥΡΓΟΣ Rain Duration", "rain_duration");


//        insertData("/home/spyros/Downloads/m_doxaro_hourly.csv", doxaroMap, "Doxaro");
        insertData("/home/spyros/Downloads/m_pyrgos_hourly.csv", pyrgosMap, "Pyrgos");
    }

    public static LocalDateTime parseDateTime(String dateString, String timeString) {
        // Define the date format pattern
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
        // Define the time format pattern
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

        // Parse the date string and return LocalDate object
        LocalDate date = LocalDate.parse(dateString, dateFormatter);
        // Parse the time string and return LocalTime object
        LocalTime time = LocalTime.parse(timeString, timeFormatter);

        // Combine date and time into LocalDateTime
        return date.atTime(time);
    }

//    public void insertData(String path, String greekTownName, String tableName) {
//
//        Town town = townRepository.findByName(tableName);
//
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
//
//        CsvTableSource csvSource = CsvTableSource.builder()
//                .path(path)
//                .fieldDelimiter(",")
//                .ignoreFirstLine()
//                .field("Date", Types.STRING)
//                .field("Time", Types.STRING)
//                .field(greekTownName + " WIND SPEED", Types.STRING)
//                .field(greekTownName + " Relative Humidity", Types.STRING)
//                .field(greekTownName + " Temperature", Types.STRING)
//                .field(greekTownName + " Barometric Pressure", Types.STRING)
//                .field(greekTownName + " Pyranometer 0 - 2000 W/m²", Types.STRING)
//                .field(greekTownName + " Precipitation", Types.STRING)
//                .field(greekTownName + " WIND DIRECTION", Types.STRING)
//                .field(greekTownName + " Hourly ETo", Types.STRING)
//                .field(greekTownName + " Rain Duration", Types.STRING)
//                .build();
//
//        Table doxaro = tEnv.fromTableSource(csvSource);
//
//        String[] fieldNames = {"DateTime", "Wind_Speed", "Relative_Humidity", "Temperature", "Barometric_Pressure",
//                "Pyronometer", "Precipitation", "WIND_DIRECTION", "Hourly_ETo", "Rain_Duration"};
//        TypeInformation<?>[] fieldTypes = {Types.LOCAL_DATE_TIME, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
//                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE};
//
//        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
//
//        DataSet<Row> result = tEnv.toDataSet(doxaro, Row.class).map(row -> {
//            Row row2 = new Row(10);
//            row2.setField(0, parseDateTime((String) row.getField(0), (String) row.getField(1)));
//            for (int i = 2; i < 11; i++) {
//                if (row.getField(i).equals("*")) {
//                    row2.setField(i - 1, 0.0);
//                    row.setField(i, 0.0);
//                }
//
//                row2.setField(i - 1, Double.parseDouble(row.getField(i).toString()));
//
//            }
//            return row2;
//        }).returns(rowTypeInfo);
//
////        try {
////            result.print();
////        } catch (Exception e) {
////            throw new RuntimeException(e);
////        }
//
//        try {
//            long startTime = System.currentTimeMillis();
//            List<Weather> weatherList = new ArrayList<>();
//
//            for (Row row : result.collect()) {
//                LocalDateTime dateTime = (LocalDateTime) row.getField(0);
//                Double windSpeed = (Double) row.getField(1);
//                Double relativeHumidity = (Double) row.getField(2);
//                Double temperature = (Double) row.getField(3);
//                Double barometricPressure = (Double) row.getField(4);
//                Double pyronometer = (Double) row.getField(5);
//                Double precipitation = (Double) row.getField(6);
//                Double windDirection = (Double) row.getField(7);
//                Double hourlyETo = (Double) row.getField(8);
//                Double rainDuration = (Double) row.getField(9);
//
//                Weather weather = Weather.builder()
//                        .dateTime(dateTime)
//                        .windSpeed(windSpeed)
//                        .relativeHumidity(relativeHumidity)
//                        .temperature(temperature)
//                        .barometricPressure(barometricPressure)
//                        .pyronometer(pyronometer)
//                        .precipitation(precipitation)
//                        .windDirection(windDirection)
//                        .hourlyETo(hourlyETo)
//                        .rainDuration(rainDuration)
//                        .town(town)
//                        .build();
//                weatherList.add(weather);
//            }
//
//            weatherRepository.saveAll(weatherList);
//            long endTime = System.currentTimeMillis();
//
//            log.info("Saved all {} entities to db in {} seconds", result.collect().size(), (endTime - startTime) / 1000.0);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//    }

    public void insertData(String path, Map<String, String> columnMapper, String tableName) {

        Town town = townRepository.findByName(tableName);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        CsvTableSource.Builder csvSource = CsvTableSource.builder()
                .path(path)
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .field("Date", Types.STRING)
                .field("Time", Types.STRING);

        int index = 0;
        for (String field : columnMapper.keySet()) {
            if (index != 0) {
                csvSource.field(field, Types.STRING);
            }
            index++;
        }

        Table doxaro = tEnv.fromTableSource(csvSource.build());
        TypeInformation<?>[] types = new TypeInformation[columnMapper.size()];
        types[0] = Types.LOCAL_DATE_TIME;
        for (int i = 1; i < types.length; i++) {
            types[i] = Types.DOUBLE;
        }
        //TODO Date Time sto columnMapper
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, columnMapper.values().toArray(String[]::new));

        DataSet<Row> result = tEnv.toDataSet(doxaro, Row.class).map(row -> {
            Row outputRow = new Row(10);
            outputRow.setField(0, parseDateTime((String) row.getField(0), (String) row.getField(1)));


//            Object[] mapperArray = linkedHashMap.values().toArray();
//            Object valueForFirstKey = columnMapper.get(firstKey);

            Object[] firstKey = columnMapper.values().toArray();
//            Object valueForFirstKey = columnMapper.get(firstKey);

            for (int i = 2; i <= types.length; i++) {

                if (row.getField(i).equals("*")) {
                    outputRow.setField(columnToIndex.get(firstKey[i - 1]), 0.0);
                    row.setField(i, 0.0);
                }


                outputRow.setField(columnToIndex.get(firstKey[i - 1]), Double.parseDouble(row.getField(i).toString()));

            }
            return outputRow;
        }).returns(rowTypeInfo);

        try {
            result.print();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            long startTime = System.currentTimeMillis();
            List<Weather> weatherList = new ArrayList<>();

            for (Row row : result.collect()) {
                LocalDateTime dateTime = (LocalDateTime) row.getField(0);
                Double windSpeed = (Double) row.getField(1);
                Double relativeHumidity = (Double) row.getField(2);
                Double temperature = (Double) row.getField(3);
                Double barometricPressure = (Double) row.getField(4);
                Double pyronometer = (Double) row.getField(5);
                Double precipitation = (Double) row.getField(6);
                Double windDirection = (Double) row.getField(7);
                Double hourlyETo = (Double) row.getField(8);
                Double rainDuration = (Double) row.getField(9);

                Weather weather = Weather.builder()
                        .dateTime(dateTime)
                        .windSpeed(windSpeed)
                        .relativeHumidity(relativeHumidity)
                        .temperature(temperature)
                        .barometricPressure(barometricPressure)
                        .pyronometer(pyronometer)
                        .precipitation(precipitation)
                        .windDirection(windDirection)
                        .hourlyETo(hourlyETo)
                        .rainDuration(rainDuration)
                        .town(town)
                        .build();
                weatherList.add(weather);
            }

            weatherRepository.saveAll(weatherList);
            long endTime = System.currentTimeMillis();

            log.info("Saved all {} entities to db in {} seconds", result.collect().size(), (endTime - startTime) / 1000.0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
