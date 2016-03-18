package com.googlecode.jmxtrans.model.output.elastic;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.googlecode.jmxtrans.model.OutputWriter;
import com.googlecode.jmxtrans.model.OutputWriterFactory;
import com.googlecode.jmxtrans.model.output.support.ResultTransformerOutputWriter;
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
		createMappingIfNeeded(jestClient, indexName, typeName);
		return ResultTransformerOutputWriter.booleanToNumber(
				booleanAsNumber,
				new ElasticsearchWriter(indexName, jestClient));
	}
}
