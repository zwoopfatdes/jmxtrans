/**
 * The MIT License
 * Copyright © 2010 JmxTrans team
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.googlecode.jmxtrans.model.output;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.googlecode.jmxtrans.exceptions.LifecycleException;
import com.googlecode.jmxtrans.model.NamingStrategy;
import com.googlecode.jmxtrans.model.OutputWriter;
import com.googlecode.jmxtrans.model.OutputWriterFactory;
import com.googlecode.jmxtrans.model.naming.ClassAttributeNamingStrategy;
import com.googlecode.jmxtrans.model.naming.JexlNamingStrategy;
import com.googlecode.jmxtrans.model.output.support.ResultTransformerOutputWriter;
import com.googlecode.jmxtrans.model.output.support.TcpOutputWriterBuilder;
import com.googlecode.jmxtrans.model.output.support.UdpOutputWriterBuilder;
import com.googlecode.jmxtrans.model.output.support.WriterPoolOutputWriter;
import com.googlecode.jmxtrans.model.output.support.influxdb.InfluxDBMessageFormatter;
import com.googlecode.jmxtrans.model.output.support.pool.FlushStrategy;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.commons.jexl2.JexlException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.googlecode.jmxtrans.model.output.support.pool.FlushStrategyUtils.createFlushStrategy;
import static java.lang.String.format;

@ThreadSafe
@EqualsAndHashCode
@ToString
public class InfluxDBWriterFactory implements OutputWriterFactory {

	public static final String UDP = "udp";
	public static final String TCP = "tcp";

	@Nonnull private final ImmutableMap<String, String> tags;
	@Nonnull private final ImmutableList<String> typeNames;
	@Nonnull private final boolean booleanAsNumber;
	@Nonnull private final String tagName;
	private final boolean mergeTypeNamesTags;
	@Nullable private final String metricNamingExpression;
	private final boolean addHostnameTag;
	private final boolean allKeysAsTags;
	@Nonnull private final InetSocketAddress server;
	@Nonnull private final FlushStrategy flushStrategy;
	@Nonnull private final String transport;
	private final int poolSize;

	@JsonCreator
	public InfluxDBWriterFactory(
			@JsonProperty("typeNames") ImmutableList<String> typeNames,
			@JsonProperty("booleanAsNumber") boolean booleanAsNumber,
			@JsonProperty("host") String host,
			@JsonProperty("port") Integer port,
			@JsonProperty("tags") Map<String, String> tags,
			@JsonProperty("tagName") String tagName,
			@JsonProperty("mergeTypeNamesTags") boolean mergeTypeNamesTags,
			@JsonProperty("metricNamingExpression") String metricNamingExpression,
			@JsonProperty("addHostnameTag") boolean addHostnameTag,
			@JsonProperty("allKeysAsTags") boolean allKeysAsTags,
			@JsonProperty("transport") String transport,
			@JsonProperty("flushStrategy") String flushStrategy,
			@JsonProperty("flushDelayInSeconds") Integer flushDelayInSeconds,
			@JsonProperty("poolSize") Integer poolSize) throws LifecycleException, UnknownHostException {
		this.typeNames = typeNames;
		this.booleanAsNumber = booleanAsNumber;
		this.tagName = checkNotNull(tagName);
		this.mergeTypeNamesTags = mergeTypeNamesTags;
		this.metricNamingExpression = metricNamingExpression;
		this.addHostnameTag = addHostnameTag;
		this.allKeysAsTags = allKeysAsTags;
		this.server = new InetSocketAddress(
				firstNonNull(host, "localhost"),
				firstNonNull(port, 3030));

		this.tags = firstNonNull(ImmutableMap.copyOf(tags), ImmutableMap.<String, String>of());

		this.transport = validateTransport(transport);
		this.flushStrategy = createFlushStrategy(flushStrategy, flushDelayInSeconds);
		this.poolSize = firstNonNull(poolSize, 1);
	}

	private NamingStrategy createMetricNameStrategy() throws LifecycleException {
		if (metricNamingExpression == null) return new ClassAttributeNamingStrategy();

		try {
			return new JexlNamingStrategy(metricNamingExpression);
		} catch (JexlException jexlExc) {
			throw new LifecycleException("failed to setup naming strategy", jexlExc);
		}
	}

	private InfluxDBMessageFormatter createMesageFormater() throws UnknownHostException, LifecycleException {
		return new InfluxDBMessageFormatter(typeNames, tags, tagName,
				createMetricNameStrategy(), mergeTypeNamesTags, allKeysAsTags,
				addHostnameTag ? InetAddress.getLocalHost().getHostName() : null);
	}

	private String validateTransport(String transport) {
		if (transport == null) return UDP;
		if (transport.equalsIgnoreCase(UDP)) return UDP;
		if (transport.equalsIgnoreCase(TCP)) return TCP;
		throw new IllegalArgumentException(format("Transport should be either %s or %s", UDP, TCP));
	}

	@Override
	@SneakyThrows
	public OutputWriter create() {
		return ResultTransformerOutputWriter.booleanToNumber(
				booleanAsNumber,
				buildTransport(new InfluxDBWriter(createMesageFormater())));
	}

	private WriterPoolOutputWriter<InfluxDBWriter> buildTransport(InfluxDBWriter influxDBWriter) {
		// NOTE: we could extract a common PooledWriterBuilder interface here
		if (transport.equalsIgnoreCase(UDP)) {
			return UdpOutputWriterBuilder.builder(server, influxDBWriter)
					.setFlushStrategy(flushStrategy)
					.setPoolSize(poolSize)
					.build();
		} else {
			return TcpOutputWriterBuilder.builder(server, influxDBWriter)
					.setFlushStrategy(flushStrategy)
					.setPoolSize(poolSize)
					.build();
		}
	}
}
