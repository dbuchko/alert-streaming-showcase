package showcase.alarm;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import lombok.extern.slf4j.Slf4j;
import nyla.solutions.core.patterns.conversion.Converter;
import nyla.solutions.core.util.Debugger;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.rabbit.stream.listener.StreamListenerContainer;
import showcase.alarm.domains.Activity;
import showcase.alarm.domains.Alert;

import java.util.Arrays;
import java.util.List;

@Configuration
@Slf4j
class RabbitAmqpConsumerConfig {

    @Value("${stream.filter.offset:FIRST}")
    private String offsetName;

    @Value("${stream.destination.alerts:alerts.alert}")
    private String alertStream;

    @Value("${stream.alert.filter.name:account}")
    private String alertFilterPropertyName;

    @Value("${stream.alert.filter.value:}")
    private String alertFilterValue;

    @Value("${stream.filter.level.name:level}")
    private String alertLevelPropertyName;

    // Comma-separated list of levels to accept, e.g. "critical,high"
    @Value("${stream.filter.levels:}")
    private String filterLevels;

    @Value("${stream.activity.filter.value:}")
    private String activityFilterValue;

    @Value("${stream.activity.filter.name:account}")
    private String activityFilterPropertyName;

    @Value("${spring.cloud.stream.bindings.input.destination:amq.topic}")
    private String alertExchangeName;

    @Value("${stream.alert.exchange.bind.key:#}")
    private String alertBindRoutingKey;

    @Value("${stream.activity.stream:activities.activity}")
    private String activityStream;

    @Value("${stream.activity.exchange:amq.topic}")
    private String activityExchangeName;

    @Value("${stream.activity.exchange.bind.key:#}")
    private String activityBindRoutingKey;


    // --- Queue / Exchange / Binding declarations (replaces Management.QueueInfo beans) ---

    @Bean("alertQueue")
    Queue alertQueue() {
        return QueueBuilder.durable(alertStream).stream().build();
    }

    @Bean("alertTopicExchange")
    TopicExchange alertTopicExchange() {
        return new TopicExchange(alertExchangeName);
    }

    @Bean
    Binding alertBinding(@Qualifier("alertQueue") Queue alertQueue,
                         @Qualifier("alertTopicExchange") TopicExchange exchange) {
        return BindingBuilder.bind(alertQueue).to(exchange).with(alertBindRoutingKey);
    }

    @Bean("activityQueue")
    Queue activityQueue() {
        return QueueBuilder.durable(activityStream).stream().build();
    }

    @Bean("activityTopicExchange")
    TopicExchange activityTopicExchange() {
        return new TopicExchange(activityExchangeName);
    }

    @Bean
    Binding activityBinding(@Qualifier("activityQueue") Queue activityQueue,
                            @Qualifier("activityTopicExchange") TopicExchange exchange) {
        return BindingBuilder.bind(activityQueue).to(exchange).with(activityBindRoutingKey);
    }


    // --- Stream listener containers (replaces native Consumer beans) ---

    @Bean
    StreamListenerContainer alertStreamContainer(
            Environment environment,
            java.util.function.Consumer<Alert> alertConsumer,
            @Qualifier("alertConverter") Converter<byte[], Alert> messageConverter) {

        var container = new StreamListenerContainer(environment);
        container.setConsumerCustomizer((name, builder) -> {
            builder.offset(resolveOffset(offsetName))
                   .noTrackingStrategy();

            List<String> levels = filterLevels.isEmpty()
                    ? List.of()
                    : Arrays.stream(filterLevels.split(","))
                            .map(s -> s.strip().toLowerCase())
                            .toList();

            log.info("Alert stream filter config — property: '{}', value: '{}', levelProperty: '{}', levels: {}, filterLevels raw: '{}'",
                    alertFilterPropertyName, alertFilterValue, alertLevelPropertyName, levels, filterLevels);

            // Broker-side bloom filter reduces network traffic; postFilter does exact matching
            builder.filter()
                   .values(alertFilterValue)
                   .postFilter(msg -> {
                       var props = msg.getApplicationProperties();

                       // Log every property key/value so we can verify names and casing
                       log.info("Message application properties: {}", props);

                       Object rawAccount = props.get(alertFilterPropertyName);
                       Object rawLevel   = props.get(alertLevelPropertyName);
                       String levelStr   = rawLevel != null ? rawLevel.toString().toLowerCase() : null;

                       boolean accountMatch = alertFilterValue.isEmpty() ||
                                             alertFilterValue.equals(rawAccount);
                       boolean levelMatch = levels.isEmpty() ||
                                           (levelStr != null && levels.contains(levelStr));

                       boolean result = accountMatch && levelMatch;
                       log.info("postFilter — {}='{}' (expected '{}') accountMatch={} | {}='{}' (expected one of {}) levelMatch={} — accepted={}",
                               alertFilterPropertyName, rawAccount, alertFilterValue, accountMatch,
                               alertLevelPropertyName, rawLevel, levels, levelMatch,
                               result);
                       return result;
                   });
        });

        container.setupMessageListener((MessageListener) msg -> {
            try {
                log.info("Processing alert from stream: {}", alertStream);
                alertConsumer.accept(messageConverter.convert(msg.getBody()));
            } catch (Exception e) {
                log.error(Debugger.stackTrace(e));
                throw e;
            }
        });
        container.setQueueNames(alertStream);
        return container;
    }

    @Bean
    StreamListenerContainer activityStreamContainer(
            Environment environment,
            java.util.function.Consumer<Activity> activityConsumer,
            @Qualifier("activityConverter") Converter<byte[], Activity> messageConverter) {

        var container = new StreamListenerContainer(environment);
        container.setConsumerCustomizer((name, builder) -> {
            builder.offset(resolveOffset(offsetName))
                   .noTrackingStrategy();

            if (!activityFilterPropertyName.isEmpty() && !activityFilterValue.isEmpty()) {
                log.info("Activity stream filter — {}: {}", activityFilterPropertyName, activityFilterValue);
                builder.filter()
                       .values(activityFilterValue)
                       .postFilter(msg -> activityFilterValue.equals(
                               msg.getApplicationProperties().get(activityFilterPropertyName)));
            }
        });

        container.setupMessageListener((MessageListener) msg -> {
            try {
                log.info("Processing activity from stream: {}", activityStream);
                activityConsumer.accept(messageConverter.convert(msg.getBody()));
            } catch (Exception e) {
                log.error(Debugger.stackTrace(e));
                throw e;
            }
        });
        container.setQueueNames(activityStream);
        return container;
    }


    // OffsetSpecification has no valueOf(String) — translate the config string explicitly
    private static OffsetSpecification resolveOffset(String name) {
        return switch (name.toUpperCase()) {
            case "LAST" -> OffsetSpecification.last();
            case "NEXT" -> OffsetSpecification.next();
            default     -> OffsetSpecification.first();
        };
    }
}
