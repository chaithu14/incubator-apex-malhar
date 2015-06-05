/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class InputItemGenerator implements InputOperator
{
  private String eventSchemaJSON;
  private DimensionalConfigurationSchema schema;

  public static final String PUBLISHER = "publisher";
  public static final String ADVERTISER = "advertiser";
  public static final String LOCATION = "location";

  public static final String IMPRESSIONS = "impressions";
  public static final String CLICKS = "clicks";
  public static final String COST = "cost";
  public static final String REVENUE = "revenue";

  public static final double EXPECTED_CLICK_THROUGH_RATE = .015;

  private int publisherID;
  private int advertiserID;
  private int locationID;

  public List<Object> publisherName;
  public List<Object> advertiserName;
  public List<Object> locationName;

  private double minuteOffset;
  private double dayOffset;
  private double expectedClickThruRate;
  private long currentMinute;
  private long currentDay;
  @Min(1)
  private int blastCount = 30000;
  @Min(1)
  private int numTuplesPerWindow = 1000;
  private transient int windowCount = 0;
  private final Random random = new Random(0);
  public final transient DefaultOutputPort<AdInfo> outputPort = new DefaultOutputPort<AdInfo>();

  private double[] publisherScaleArray;
  private double[] advertiserScaleArray;
  private double[] locationScaleArray;

  public double getExpectedClickThruRate()
  {
    return expectedClickThruRate;
  }

  public void setExpectedClickThruRate(double expectedClickThruRate)
  {
    this.expectedClickThruRate = expectedClickThruRate;
  }

  public int getBlastCount()
  {
    return blastCount;
  }

  public void setBlastCount(int blastCount)
  {
    this.blastCount = blastCount;
  }

  @Override
  public void beginWindow(long windowId)
  {
    windowCount = 0;
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();

    schema = new DimensionalConfigurationSchema(eventSchemaJSON,
                                        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    publisherID = schema.getKeysToEnumValuesList().get(PUBLISHER).size();
    if(advertiserName == null) {
      advertiserID = schema.getKeysToEnumValuesList().get(ADVERTISER).size();
    }
    else {
      advertiserID = advertiserName.size();
    }
    locationID = schema.getKeysToEnumValuesList().get(LOCATION).size();

    publisherName = schema.getKeysToEnumValuesList().get(PUBLISHER);
    if(advertiserName == null) {
      advertiserName = schema.getKeysToEnumValuesList().get(ADVERTISER);
    }
    locationName = schema.getKeysToEnumValuesList().get(LOCATION);

    publisherScaleArray = new double[publisherID];
    initializeScaleArray(publisherScaleArray);
    advertiserScaleArray = new double[advertiserID];
    initializeScaleArray(advertiserScaleArray);
    locationScaleArray = new double[locationID];
    initializeScaleArray(locationScaleArray);
  }

  private void initializeScaleArray(double[] scaleArray)
  {
    for(int index = 0;
        index < scaleArray.length;
        index++) {
      scaleArray[index] = 1.0 + random.nextDouble() * 3;
    }
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    long time = System.currentTimeMillis();
    long nextMinute = time % 60000;
    long nextDay = time % (24 * 60 * 60 * 1000);
    double dayTimeOffset = .2 * (Math.cos(2.0 * Math.PI / (24.0 * 60.0 * 60.0 * 1000.0) * ((double) nextDay)) *.5 + 1.0);

    if(nextDay != currentDay) {
      currentDay = nextDay;
      dayOffset = random.nextDouble() * .05;
    }

    if(nextMinute != currentMinute) {
      expectedClickThruRate = random.nextDouble() * .01 + EXPECTED_CLICK_THROUGH_RATE;
      currentMinute = nextMinute;
      minuteOffset = random.nextDouble() * .2;
    }

    try {
      long timestamp;
      for (int i = 0; i < blastCount && windowCount < numTuplesPerWindow; ++i, windowCount++) {
        int advertiserId = random.nextInt(advertiserID);
        int publisherId = random.nextInt(publisherID);
        int adUnit = random.nextInt(locationID);

        timestamp = System.currentTimeMillis();

        double tempScale = publisherScaleArray[publisherId] *
                           advertiserScaleArray[advertiserId] *
                           locationScaleArray[adUnit];

        double cost = 0.1 + 0.05 * random.nextDouble() * tempScale + dayTimeOffset + dayOffset + minuteOffset;
        /* 0 (zero) is used as the invalid value */
        buildAndSend(false, publisherId, advertiserId, adUnit, cost, timestamp);

        if (random.nextDouble() < expectedClickThruRate) {
          double revenue = 0.1 + 0.1 * random.nextDouble() * tempScale + dayTimeOffset + dayOffset + minuteOffset;
          // generate fake click
          buildAndSend(true, publisherId, advertiserId, adUnit, revenue, timestamp);
        }
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void emitTuple(AdInfo adInfo) {
    this.outputPort.emit(adInfo);
  }

  private void buildAndSend(boolean click, int publisherId, int advertiserId, int adUnit, double value, long timestamp)
  {
    AdInfo adInfo = new AdInfo();

    adInfo.setPublisher((String) publisherName.get(publisherId));
    adInfo.publisherID = publisherId;
    adInfo.setAdvertiser((String) advertiserName.get(advertiserId));
    adInfo.advertiserID = advertiserId;
    adInfo.setLocation((String) locationName.get(adUnit));
    adInfo.locationID = adUnit;

    if (click) {
      adInfo.setRevenue(value);
      adInfo.setClicks(1L);
    }
    else {
      adInfo.setCost(value);
      adInfo.setImpressions(1);
    }
    adInfo.setTime(timestamp);
    emitTuple(adInfo);
  }

  /**
   * @return the numTuplesPerWindow
   */
  public int getNumTuplesPerWindow()
  {
    return numTuplesPerWindow;
  }

  /**
   * @param numTuplesPerWindow the numTuplesPerWindow to set
   */
  public void setNumTuplesPerWindow(int numTuplesPerWindow)
  {
    this.numTuplesPerWindow = numTuplesPerWindow;
  }

  /**
   * @return the eventSchemaJSON
   */
  public String getEventSchemaJSON()
  {
    return eventSchemaJSON;
  }

  /**
   * @param eventSchemaJSON the eventSchemaJSON to set
   */
  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = eventSchemaJSON;
  }

  private static final Logger LOG = LoggerFactory.getLogger(InputItemGenerator.class);
}
