/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.Context;

/**
 *
 * @param <K>
 * @param <V>
 * @param <CONTEXT>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class EmbeddedWebSocketSchemaServer<K, V, CONTEXT extends Context> extends EmbeddedWebSocketServer<Map<K, V>, CONTEXT>
{
  public static enum MESSAGE_TYPE
  {
    SUBSCRIBE,
    UNSUBSCRIBE,
    QUERY
  }

  private HashMap<Integer, ArrayList<EmbeddedWebSocket>> schemaSubscribers = new HashMap<Integer, ArrayList<EmbeddedWebSocket>>();

  @Override
  public void process(Map<K, V> t)
  {
    int schemaId = (Integer)t.get("schemaId");
    String out = "hello world";
    for (EmbeddedWebSocket subscriber : schemaSubscribers.get(schemaId)) {
      sendMessage(subscriber, out);
    }
  }

  @Override
  protected void processMessage(EmbeddedWebSocket client, String query)
  {
    /**
     * subscription protocol for schema
     * 1. on subscribe, add client to the specified schema id
     * 2. on unsubscribe, remove client from the specified schema id
     * 3. on query, send query to server and send response back to client
     */
    MESSAGE_TYPE type = MESSAGE_TYPE.SUBSCRIBE;
    ArrayList<Integer> schemaIds = new ArrayList<Integer>();

    switch (type) {
      case QUERY:
        // on query
        String response = callBackBase.onQuery(query);
        sendMessage(client, response);
        break;
      case SUBSCRIBE:
        // on subscribe
        for (Integer schemaId : schemaIds) {
          ArrayList<EmbeddedWebSocket> subscribers = schemaSubscribers.get(schemaId);
          if (subscribers == null) {
            subscribers = new ArrayList<EmbeddedWebSocket>();
            schemaSubscribers.put(schemaId, subscribers);
          }
          subscribers.add(client);
        }
        break;
      case UNSUBSCRIBE:
        // on unsubscribe
        for (Integer schemaId : schemaIds) {
          ArrayList<EmbeddedWebSocket> subscribers = schemaSubscribers.get(schemaId);
          if (subscribers != null) {
            subscribers.remove(client);
            if (subscribers.isEmpty()) {
              schemaSubscribers.remove(schemaId);
            }
          }
        }
        break;
      default:
        sendMessage(client, "INVALID TYPE");
    }

  }

}