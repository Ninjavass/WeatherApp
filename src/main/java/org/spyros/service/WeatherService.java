package org.spyros.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.spyros.model.Weather;
import org.spyros.repository.WeatherRepository;

import java.util.List;

@Service
@AllArgsConstructor
@Slf4j
public class WeatherService {
    private final WeatherRepository weatherRepository;

    public List<Weather> getAllData() {
        return weatherRepository.findAllLimitedToTwenty();
    }

    public List<Weather> getWeatherByTown(int townId) {
        return weatherRepository.findAllByTownLimitedToTwenty(townId);
    }

}
