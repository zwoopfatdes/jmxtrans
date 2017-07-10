/**
 * The MIT License
 * Copyright © 2010 JmxTrans team
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.googlecode.jmxtrans.model.output;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.googlecode.jmxtrans.model.*;
import com.googlecode.jmxtrans.model.naming.KeyUtils;
import com.googlecode.jmxtrans.model.naming.typename.TypeNameValue;
import org.apache.commons.lang.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Maps.newHashMap;
import static com.googlecode.jmxtrans.util.NumberUtils.isValidNumber;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * {@link com.googlecode.jmxtrans.model.OutputWriter} for
 * <a href="https://influxdb.com/index.html">InfluxDB</a>.
 *
 * @author Simon Hutchinson
 *         <a href="https://github.com/sihutch">github.com/sihutch</a>
 */
@ThreadSafe
public class InfluxDbWriter extends OutputWriterAdapter {
    private static final Logger log = LoggerFactory.getLogger(InfluxDbWriter.class);

    public static final String TAG_HOSTNAME = "hostname";

    @Nonnull
    private final InfluxDB influxDB;
    @Nonnull
    private final String database;
    @Nonnull
    private final ConsistencyLevel writeConsistency;
    @Nonnull
    private final String retentionPolicy;
    @Nonnull
    private final ImmutableMap<String, String> tags;
    @Nonnull
    ImmutableList<String> typeNames;
    @Nonnull
    private final boolean typeNamesAsTags;
    @Nonnull
    private final Pattern topicPattern;

    /**
     * The {@link ImmutableSet} of {@link ResultAttribute} attributes of
     * {@link Result} that will be written as {@link Point} tags
     */
    private final ImmutableSet<ResultAttribute> resultAttributesToWriteAsTags;

    private final boolean createDatabase;

    private final Predicate<Object> isNotNaN = new Predicate<Object>() {
        @Override
        public boolean apply(Object input) {
            return !input.toString().equals("NaN");
        }
    };

    public InfluxDbWriter(
            @Nonnull InfluxDB influxDB,
            @Nonnull String database,
            @Nonnull ConsistencyLevel writeConsistency,
            @Nonnull String retentionPolicy,
            @Nonnull ImmutableMap<String, String> tags,
            @Nonnull boolean typeNamesAsTags,
            @Nonnull boolean topicAttribute,
            @Nonnull ImmutableSet<ResultAttribute> resultAttributesToWriteAsTags,
            @Nonnull ImmutableList<String> typeNames,
            boolean createDatabase) {
        this.typeNames = typeNames;
        this.database = database;
        this.writeConsistency = writeConsistency;
        this.retentionPolicy = retentionPolicy;
        this.influxDB = influxDB;
        this.tags = tags;
        this.typeNamesAsTags = typeNamesAsTags;
        this.resultAttributesToWriteAsTags = resultAttributesToWriteAsTags;
        this.createDatabase = createDatabase;

        if (topicAttribute) {
            topicPattern = Pattern.compile("([A-Z]+)_(.+)_(v\\d+)-(\\d+)[_.](.+)");
        } else {
            topicPattern = null;
        }
    }

    /**
     * <p>
     * Each {@link Result} is written as a {@link Point} to InfluxDB
     * </p>
     * <p>
     * <p>
     * The measurement for the {@link Point} is to {@link Result#getKeyAlias()}
     * <p>
     * <a href=
     * "https://influxdb.com/docs/v0.9/concepts/key_concepts.html#retention-policy">
     * The retention policy</a> for the measurement is set to "default" unless
     * overridden in settings:
     * </p>
     * <p>
     * <p>
     * The write consistency level defaults to "ALL" unless overridden in
     * settings:
     * <p>
     * <ul>
     * <li>ALL = Write succeeds only if write reached all cluster members.</li>
     * <li>ANY = Write succeeds if write reached any cluster members.</li>
     * <li>ONE = Write succeeds if write reached at least one cluster members.
     * </li>
     * <li>QUORUM = Write succeeds only if write reached a quorum of cluster
     * members.</li>
     * </ul>
     * <p>
     * <p>
     * The time key for the {@link Point} is set to {@link Result#getEpoch()}
     * </p>
     * <p>
     * <p>
     * All {@link Result#getValues()} are written as fields to the {@link Point}
     * </p>
     * <p>
     * <p>
     * The following properties from {@link Result} are written as tags to the
     * {@link Point} unless overriden in settings:
     * <p>
     * <ul>
     * <li>{@link Result#getAttributeName()}</li>
     * <li>{@link Result#getClassName()}</li>
     * <li>{@link Result#getObjDomain()}</li>
     * <li>{@link Result#getTypeName()}</li>
     * </ul>
     * <p>
     * {@link Server#getHost()} is set as a tag on every {@link Point}
     * </p>
     */
    @Override
    public void doWrite(Server server, Query query, Iterable<Result> results) throws Exception {
        // Creates only if it doesn't already exist
        if (createDatabase) influxDB.createDatabase(database);
        BatchPoints.Builder batchPointsBuilder = BatchPoints.database(database).retentionPolicy(retentionPolicy)
                .tag(TAG_HOSTNAME, server.getSource());

        for (Map.Entry<String, String> tag : tags.entrySet()) {
            batchPointsBuilder.tag(tag.getKey(), tag.getValue());
        }

        BatchPoints batchPoints = batchPointsBuilder.consistency(writeConsistency).build();
        for (Result result : results) {
            log.debug("Query result: {}", result);

            Map<String, Object> resultValues = result.getValues();

            Map<String, String> topicTags = newHashMap();
            String topicAttributeName = "";
            if (topicPattern != null) {
                String attributeName = result.getAttributeName();
                Matcher matcher = topicPattern.matcher(attributeName);
                if (matcher.matches()) {
                  if (matcher.groupCount() == 5) {
                      topicTags.put("topicFullName", matcher.group(1) + "_" + matcher.group(2) + "_" + matcher.group(3));
                      topicTags.put("topicPrefix", matcher.group(1));
                      topicTags.put("topicName", matcher.group(2));
                      topicTags.put("topicVersion", matcher.group(3));
                      topicTags.put("topicPartition", matcher.group(4));

                      topicAttributeName = matcher.group(5);
                   }
                }
            }

            HashMap<String, Object> filteredValues = newHashMap();
            for (Map.Entry<String, Object> values : resultValues.entrySet()) {
                Object value = values.getValue();
                if (isValidNumber(value)) {
                    if (StringUtils.isBlank(topicAttributeName)) {
                        String key = KeyUtils.getPrefixedKeyString(query, result, values, typeNames, values.getKey());
                        filteredValues.put(key, value);
                    } else {
                        filteredValues.put(topicAttributeName, value);
                    }
                }
            }

            // send the point if filteredValues isn't empty
            if (!filteredValues.isEmpty()) {
                Map<String, String> resultTagsToApply = buildResultTagMap(result);

                if (typeNamesAsTags) {
                    Map<String, String> typeNameMap = TypeNameValue.extractMap(result.getTypeName());
                    for (Map.Entry<String, String> typeName : typeNameMap.entrySet()) {
                        resultTagsToApply.put("typeName-" + typeName.getKey(), typeName.getValue());
                    }
                }

                resultTagsToApply.putAll(topicTags);

                Point point = Point.measurement(result.getKeyAlias()).time(result.getEpoch(), MILLISECONDS)
                        .tag(resultTagsToApply).fields(filteredValues).build();

                log.debug("Point: {}", point);
                batchPoints.point(point);
            }
        }

        influxDB.write(batchPoints);
    }

    private Map<String, String> buildResultTagMap(Result result) throws Exception {

        Map<String, String> resultTagMap = new TreeMap<>();
        for (ResultAttribute resultAttribute : resultAttributesToWriteAsTags) {
            resultAttribute.addAttribute(resultTagMap, result);
        }

        return resultTagMap;

    }

}
