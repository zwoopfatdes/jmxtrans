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

public class ElasticsearchWriter extends OutputWriterAdapter {

	private static final Logger log = LoggerFactory.getLogger(ElasticsearchWriter.class);

	@Nonnull private final JestClient jestClient;
	@Nonnull private final String indexName;
	@Nonnull private final String typeName;

	public ElasticsearchWriter(@Nonnull JestClient jestClient, @Nonnull String indexName, @Nonnull String typeName) {
		this.jestClient = jestClient;
		this.indexName = indexName;
		this.typeName = typeName;
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

				log.debug("Insert into Elastic: Index: [{}] Type: [{}] Map: [{}]", indexName, typeName, map);
				Index index = new Index.Builder(map).index(indexName).type(typeName).build();
				JestResult addToIndex = jestClient.execute(index);
				if (!addToIndex.isSucceeded()) {
					throw new ElasticWriterException(String.format("Unable to write entry to elastic: %s", addToIndex.getErrorMessage()));
				}
			}
		}

	}

}
