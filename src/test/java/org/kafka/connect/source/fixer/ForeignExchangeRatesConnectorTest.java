package org.kafka.connect.source.fixer;


import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ForeignExchangeRatesConnectorTest {
    private static final String API_ENDPOINT = "url";
    private static final String TOPIC_NAME = "topic";

    private ForeignExchangeRatesConnector connector;
    private ConnectorContext context;
    private Map<String, String> sourceProperties;

    @Before
    public void setup() {
        connector = new ForeignExchangeRatesConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);

        sourceProperties = new HashMap<>();
        sourceProperties.put(TOPIC_NAME, "xe");
        sourceProperties.put(API_ENDPOINT, "fakeurl.com/path");

    }

    @Test
    public void testSourceTasks() {
        PowerMock.replayAll();
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        Assert.assertEquals(1, taskConfigs.size());
        Assert.assertEquals("xe", taskConfigs.get(0).get("topic"));
        Assert.assertEquals("fakeurl.com/path", taskConfigs.get(0).get(API_ENDPOINT));

        PowerMock.verifyAll();
    }

    @Test public void testMultipleSources(){
        PowerMock.replayAll();
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(100);
        Assert.assertEquals(1, taskConfigs.size());
        PowerMock.verifyAll();
    }

    @Test
    public void testTaskClass() {
        PowerMock.replayAll();
        connector.start(sourceProperties);
        Assert.assertEquals(ForeignExchangeRatesTask.class, connector.taskClass());
        PowerMock.verifyAll();
    }
}
