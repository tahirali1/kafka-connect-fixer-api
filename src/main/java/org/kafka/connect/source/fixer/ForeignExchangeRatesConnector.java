package org.kafka.connect.source.fixer;


import com.google.common.base.Strings;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.errors.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the Connector to pull Foreign currency exchange rates
 * and store in kafka topic
 * @author tahir ali
 */
public class ForeignExchangeRatesConnector extends SourceConnector {

    private static final ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define("topic",
                            ConfigDef
                                    .Type
                                    .STRING,
                            ConfigDef.Importance.LOW,
                            "The topic to publish data to");

    private static final String API_ENDPOINT = "url";
    private static final String TOPIC_NAME = "topic";

    private String url;
    private String topicName;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Starts the connector and check that the provider configuration is complete.
     */
    @Override
    public void start(final Map<String, String> map) {
        topicName = map.get(TOPIC_NAME);
        url = map.get(API_ENDPOINT);

        if (Strings.isNullOrEmpty(topicName)) {
            throw new ConnectException("ForeignExchangeRatesConnector "
                    + "configuration must include 'topic' setting");
        }
        if (topicName.contains(",")) {
            throw new ConnectException("ForeignExchangeRatesConnector "
                    + "should only have a single topic when used as a source.");
        }
        if(Strings.isNullOrEmpty(url)) {
            throw new ConnectException("ForeignExchangeRatesConnector "
                    + "configuration must include 'API' setting.");
        }
    }

    /**
     * Create configurations for the tasks based on the current configuration of the Connector.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return ForeignExchangeRatesTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTask) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();

        Map<String, String> config = new HashMap<>();
        config.put(API_ENDPOINT,url);
        config.put(TOPIC_NAME, topicName);
        configs.add(config);

        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        ConfigDef configDef = config();
        System.out.println("validating config");
        List<ConfigValue> configValues = configDef.validate(connectorConfigs);
        return new Config(configValues);
    }
}
