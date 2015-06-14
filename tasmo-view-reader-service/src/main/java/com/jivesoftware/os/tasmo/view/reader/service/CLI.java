/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */

package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientException;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.HttpResponse;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class CLI {
    public static void main(String[] args) throws HttpClientException, IOException {

        Collection<HttpClientConfiguration> configurations = Lists.newArrayList();
        HttpClientFactory createHttpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(configurations);

        HttpClient client = createHttpClientFactory.createClient("10.5.100.136", 50_000);

        GetTheseIds request = new GetTheseIds();
        request.tenantId = "event-generator";
        request.ids = Lists.newArrayList();
        for (int i = 0; i < 3_000; i++) {
            request.ids.add(new ObjectId("SearchUserView_", new Id(i)));
        }

        ObjectMapper mapper = new ObjectMapper();
        long start = System.currentTimeMillis();
        HttpResponse postJson = client.postJson("/view/get", mapper.writeValueAsString(request));

        ArrayNode entries = mapper.readValue(new ByteArrayInputStream(postJson.getResponseBody()), ArrayNode.class);
        for (int i = 0; i < entries.size(); i++) {
            System.out.println(request.ids.get(i) + ":" + entries.get(i));
        }
        System.out.println("Elapse:" + (System.currentTimeMillis() - start));

    }

    static class GetTheseIds {
        public String tenantId;
        public List<ObjectId> ids;
        public boolean includeDeletedEntities = false;

        @Override
        public String toString() {
            return "GetTheseIds{" + "tenantId=" + tenantId + ", ids=" + ids + ", includeDeletedEntities=" + includeDeletedEntities + '}';
        }

    }
}
