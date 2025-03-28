/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.bruteforce;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;

public class BruteForceEngineIT extends OpenSearchRestTestCase {

    public void testBruteForceEngine() throws IOException, ParseException {
        Request request = new Request("GET", "/_cat/plugins");
        request.addParameter("v", "true");
        Response response = client().performRequest(request);
        logger.info("Response: {}", EntityUtils.toString(response.getEntity()));
    }

}
