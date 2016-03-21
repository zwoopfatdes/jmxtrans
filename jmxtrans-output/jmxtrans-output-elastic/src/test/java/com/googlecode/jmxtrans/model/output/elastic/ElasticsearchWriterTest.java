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
