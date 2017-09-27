package org.kafka.connect.source.fixer;


import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * A class implementing the functionality for connecting with fixer.io API
 * @author tahir ali
 */
public class FixerClient {
    private WebTarget webTarget;
    public FixerClient(final WebTarget webTarget) {
        this.webTarget = webTarget;
    }

    /**
     * invoke the Fixer api.
     * @throws ProcessingException generic failure in processing the request
     */
    public String executeRequest() throws ProcessingException {
        Response response;
        response = webTarget
                .path("latest")
                .queryParam("base","USD")
                .request(MediaType.APPLICATION_JSON)
                .get();
        if(response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(String.class);
        }
        return String.valueOf(response.getStatus());
    }
}
