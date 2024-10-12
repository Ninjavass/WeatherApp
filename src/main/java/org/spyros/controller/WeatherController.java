package org.spyros.controller;

import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.spyros.model.Weather;
import org.spyros.service.*;

import java.util.List;
@AllArgsConstructor
@RestController
@RequestMapping("/weather")
public class WeatherController {

    private final WeatherService weatherService;

    @GetMapping("/all")
    public List<Weather> getAllSavedData(){
       return weatherService.getAllData();
    }

    @GetMapping("/town/{townId}")
    public ResponseEntity<?> getWeatherByTown(@PathVariable int townId) {
        List<Weather> weatherList = weatherService.getWeatherByTown(townId);
        if (weatherList.isEmpty()) {
            String message = "No Weather found for town: " + townId;
            return new ResponseEntity<>(message, HttpStatus.NOT_FOUND);
        }
        return ResponseEntity.ok(weatherList);
    }

}

