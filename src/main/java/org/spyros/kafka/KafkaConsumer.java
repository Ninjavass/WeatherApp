package org.spyros.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import org.spyros.model.Weather;
import org.spyros.model.WeatherJson;

import java.io.IOException;
import java.util.Properties;

import static java.lang.Double.parseDouble;

@Component
public class KafkaConsumer implements ApplicationListener<ContextRefreshedEvent> {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.datasource.url}")
    private String dbUrl;

    @Value("${spring.datasource.username}")
    private String dbUsername;

    @Value("${spring.datasource.password}")
    private String dbPassword;

    private Thread consumerThread;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        startKafkaConsumerThread();
    }

    private void startKafkaConsumerThread() {
        consumerThread = new Thread(() -> {
            try {
                System.out.println("Starting Kafka consumer on thread: " + Thread.currentThread().getName());
                runJob();
            } catch (Exception e) {
                System.err.println("Error in Kafka consumer thread: " + e.getMessage());
                e.printStackTrace();
            }
        });
        consumerThread.setName("Kafka-Consumer-Thread");
        consumerThread.start();
    }

    public void runJob() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group-" + System.currentTimeMillis());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        FlinkKafkaConsumer<WeatherJson> consumer = new FlinkKafkaConsumer<>(
                "weather-topic",
                new WeatherJsonDeserializationSchema(),
                properties
        );

        DataStream<WeatherJson> stream = env.addSource(consumer);

        WeatherJsonToWeatherMapper weatherJsonToWeatherMapper = new WeatherJsonToWeatherMapper();
        DataStream<Weather> weatherStream = stream.map(weatherJsonToWeatherMapper);

        SinkFunction<Weather> jdbcSink = JdbcSink.sink(
                "INSERT INTO weather (date_time, wind_speed, relative_humidity, temperature, barometric_pressure, pyronometer, precipitation, wind_direction, hourly_eto, rain_duration, town_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (statement, weather) -> {
                    statement.setObject(1, weather.getDateTime());
                    statement.setDouble(2, weather.getWindSpeed());
                    statement.setDouble(3, weather.getRelativeHumidity());
                    statement.setDouble(4, weather.getTemperature());
                    statement.setDouble(5, weather.getBarometricPressure());
                    statement.setDouble(6, weather.getPyronometer());
                    statement.setDouble(7, weather.getPrecipitation());
                    statement.setDouble(8, weather.getWindDirection());
                    statement.setDouble(9, weather.getHourlyEto());
                    statement.setDouble(10, weather.getRainDuration());
                    statement.setLong(11, weather.getTownId()); // Assuming Town has an id field
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(dbUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(dbUsername)
                        .withPassword(dbPassword)
                        .build()
        );

        weatherStream.addSink(jdbcSink);

        env.execute("Process Weather Data");

    }

    static double parseDoubleOrDefault(String value, double defaultValue) {
        if (value == null || value.equals("*")) {
            return defaultValue;
        }
        try {
            return parseDouble(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static class WeatherJsonDeserializationSchema implements DeserializationSchema<WeatherJson> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        static {
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }

        @Override
        public WeatherJson deserialize(byte[] message) throws IOException {
//            String JsonMessage = new String(message);
//            System.out.println("Received "+ JsonMessage);
            return objectMapper.readValue(message, WeatherJson.class);
        }

        @Override
        public boolean isEndOfStream(WeatherJson nextElement) {
            return false;
        }

        @Override
        public TypeInformation<WeatherJson> getProducedType() {
            return TypeInformation.of(WeatherJson.class);
        }
    }
}