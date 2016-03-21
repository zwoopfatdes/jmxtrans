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

import com.google.common.io.Resources;
import com.googlecode.jmxtrans.model.OutputWriter;
import com.googlecode.jmxtrans.model.OutputWriterFactory;
import com.googlecode.jmxtrans.model.output.support.ResultTransformerOutputWriter;
import com.googlecode.jmxtrans.util.SystemClock;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.PutMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URL;

import static com.google.common.base.Charsets.UTF_8;

public class ElasticsearchWriterFactory implements OutputWriterFactory {

	private static final Logger log = LoggerFactory.getLogger(ElasticsearchWriterFactory.class);

	private final boolean booleanAsNumber;
	@Nonnull  private final String connectionUrl;
	static final String ELASTIC_TYPE_NAME = "jmx-entry";

	public ElasticsearchWriterFactory(boolean booleanAsNumber, @Nonnull String connectionUrl) {
		this.booleanAsNumber = booleanAsNumber;
		this.connectionUrl = connectionUrl;
	}

	@Nonnull
	private JestClient createJestClient(@Nonnull String connectionUrl) {
		log.info("Create a jest elastic search client for connection url [{}]", connectionUrl);
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(
				new HttpClientConfig.Builder(connectionUrl)
						.multiThreaded(true)
						.build());
		return factory.getObject();
	}

	private synchronized void createMappingIfNeeded(
			@Nonnull JestClient jestClient,
			@Nonnull String indexName,
			@Nonnull String typeName) throws IOException {

		IndicesExists indicesExists = new IndicesExists.Builder(indexName).build();
		boolean indexExists = jestClient.execute(indicesExists).isSucceeded();

		if (!indexExists) {
			CreateIndex createIndex = new CreateIndex.Builder(indexName).build();
			jestClient.execute(createIndex);

			URL url = ElasticWriter.class.getResource("/elastic-mapping.json");
			String mapping = Resources.toString(url, UTF_8);

			PutMapping putMapping = new PutMapping.Builder(indexName, typeName, mapping).build();

			JestResult result = jestClient.execute(putMapping);

			if (!result.isSucceeded()) {
				throw new IOException(String.format("Failed to create mapping: %s", result.getErrorMessage()));
			} else {
				log.info("Created mapping for index {}", indexName);
			}
		}
	}

	@Override
	public OutputWriter create() {
		JestClient jestClient = createJestClient(connectionUrl);
		String indexName = "";
//		createMappingIfNeeded(jestClient, indexName, ELASTIC_TYPE_NAME);
		IndexNamer indexNamer = IndexNamer.createIndexNamer(indexName, true, new SystemClock());
		return ResultTransformerOutputWriter.booleanToNumber(
				booleanAsNumber,
				new ElasticsearchWriter(jestClient, indexNamer, ELASTIC_TYPE_NAME));
	}
}
