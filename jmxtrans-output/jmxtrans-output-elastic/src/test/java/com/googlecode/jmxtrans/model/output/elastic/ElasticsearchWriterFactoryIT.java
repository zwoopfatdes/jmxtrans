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

import com.googlecode.jmxtrans.test.IntegrationTest;
import com.googlecode.jmxtrans.test.RequiresIO;
import com.kaching.platform.testing.AllowLocalFileAccess;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class, RequiresIO.class})
@AllowLocalFileAccess(paths = "*")
public class ElasticsearchWriterFactoryIT extends ESIntegTestCase {

	@Override
	protected Settings nodeSettings(int nodeOrdinal) {
		return Settings.settingsBuilder()
				.put(super.nodeSettings(nodeOrdinal))
				.put(Node.HTTP_ENABLED, true)
				.build();
	}

	@Test
	public void toto() {
		ElasticsearchWriterFactory factory = new ElasticsearchWriterFactory(true, getHttpEndpoint(), false, "jmx-index");
		factory.create();
		Assertions.assertThat(indexExists("jmx-index")).isTrue();
	}

	private String getHttpEndpoint() {
		return "http://" + internalCluster()
				.getDataNodeInstance(HttpServerTransport.class)
				.boundAddress().publishAddress().getAddress();
	}
}
