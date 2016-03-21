package com.googlecode.jmxtrans.model.output.elastic;

import com.google.gson.Gson;
import com.googlecode.jmxtrans.util.ManualClock;
import io.searchbox.client.JestClient;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.googlecode.jmxtrans.model.QueryFixtures.dummyQuery;
import static com.googlecode.jmxtrans.model.ResultFixtures.singleFalseResult;
import static com.googlecode.jmxtrans.model.ResultFixtures.singleNumericResult;
import static com.googlecode.jmxtrans.model.ServerFixtures.dummyServer;
import static com.googlecode.jmxtrans.model.output.elastic.ElasticsearchWriterFactory.ELASTIC_TYPE_NAME;
import static com.googlecode.jmxtrans.model.output.elastic.IndexNamer.createIndexNamer;
import static net.javacrumbs.jsonunit.fluent.JsonFluentAssert.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchWriterTest {

	@Mock public JestClient jestClient;
	@Mock public DocumentResult resultSucceded;
	@Mock public DocumentResult resultFailed;
	@Captor public ArgumentCaptor<Index> indexCaptor;
	private ElasticsearchWriter writer;

	@Before
	public void setupElasticsearchWriter() {
		writer = new ElasticsearchWriter(
				jestClient,
				createIndexNamer("index-name", false, new ManualClock()),
				ELASTIC_TYPE_NAME);
	}

	@Before
	public void setupResults() {
		when(resultSucceded.isSucceeded()).thenReturn(true);
		when(resultFailed.isSucceeded()).thenReturn(false);
	}

	@Test
	public void resultContextIsSentToElasticsearch() throws Exception {
		when(jestClient.execute(indexCaptor.capture())).thenReturn(resultSucceded);

		writer.doWrite(dummyServer(), dummyQuery(), singleNumericResult());

		Index index = indexCaptor.getValue();

		assertThat(index.getIndex()).isEqualTo("index-name");
		assertThat(index.getType()).isEqualTo(ELASTIC_TYPE_NAME);

		String data = index.getData(new Gson());

		assertThatJson(data)
				.node("server").isEqualTo("host.example.net")
//				.node("port").isEqualTo("'4321'")
				.node("objDomain").isEqualTo("ObjectDomainName")
				.node("className").isEqualTo("sun.management.MemoryImpl")
				.node("typeName").isEqualTo("type=Memory")
				.node("attributeName").isEqualTo("ObjectPendingFinalizationCount")
				.node("key").isEqualTo("ObjectPendingFinalizationCount")
				.node("keyAlias").isEqualTo("ObjectPendingFinalizationCount")
				.node("timestamp").isEqualTo("0");
	}

	@Test
	public void canSendNumericValues() throws Exception {
		when(jestClient.execute(indexCaptor.capture())).thenReturn(resultSucceded);

		writer.doWrite(dummyServer(), dummyQuery(), singleNumericResult());

		Index index = indexCaptor.getValue();
		String data = index.getData(new Gson());

		assertThatJson(data)
				.node("value").isEqualTo(10.0);
	}

	@Test
	public void nonNumericValuesAreNotSent() throws Exception {
		writer.doWrite(dummyServer(), dummyQuery(), singleFalseResult());

		verify(jestClient, never()).execute(any(Index.class));
	}

	@Test(expected = ElasticWriterException.class)
	public void exceptionIsThrownOnElasticsearchError() throws Exception {
		when(jestClient.execute(any(Index.class))).thenReturn(resultFailed);

		writer.doWrite(dummyServer(), dummyQuery(), singleNumericResult());
	}

}
