/**
 * The MIT License
 * Copyright (c) 2010 JmxTrans team
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
package com.googlecode.jmxtrans.model.output.elastic;

import com.googlecode.jmxtrans.model.OutputWriterAdapter;
import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.model.Server;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.googlecode.jmxtrans.util.NumberUtils.isNumeric;
import static java.lang.String.format;

public class ElasticsearchWriter extends OutputWriterAdapter {

	private static final Logger log = LoggerFactory.getLogger(ElasticsearchWriter.class);

	@Nonnull private final JestClient jestClient;
	@Nonnull private final IndexNamer indexNamer;
	@Nonnull private final String type;

	public ElasticsearchWriter(@Nonnull JestClient jestClient, @Nonnull IndexNamer indexNamer, @Nonnull String type) {
		this.jestClient = jestClient;
		this.indexNamer = indexNamer;
		this.type = type;
	}

	@Override
	public void doWrite(Server server, Query query, Iterable<Result> results) throws Exception {
		for (Result result : results) {
			log.debug("Query result: [{}]", result);
			Map<String, Object> resultValues = result.getValues();
			for (Map.Entry<String, Object> values : resultValues.entrySet()) {
				Object value = values.getValue();
				if (!isNumeric(value)) {
					log.warn("Unable to submit non-numeric value to Elastic: [{}] from result [{}]", value, result);
					break;
				}
				Map<String, Object> map = newHashMap();
				map.put("serverAlias", server.getAlias());
				map.put("server", server.getHost());
				map.put("port", server.getPort());
				map.put("objDomain", result.getObjDomain());
				map.put("className", result.getClassName());
				map.put("typeName", result.getTypeName());
				map.put("attributeName", result.getAttributeName());
				map.put("key", values.getKey());
				map.put("keyAlias", result.getKeyAlias());
				map.put("value", Double.parseDouble(value.toString()));
				map.put("timestamp", result.getEpoch());

				log.debug("Insert into Elastic: Index: [{}] Type: [{}] Map: [{}]", indexNamer, type, map);
				Index index = new Index.Builder(map).index(indexNamer.getName()).type(type).build();
				JestResult addToIndex = jestClient.execute(index);
				if (!addToIndex.isSucceeded()) {
					throw new ElasticWriterException(format("Unable to write entry to elastic: %s", addToIndex.getErrorMessage()));
				}
			}
		}

	}

}
