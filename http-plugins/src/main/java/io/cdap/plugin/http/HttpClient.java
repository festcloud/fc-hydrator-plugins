/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import io.cdap.plugin.actions.HTTPArgumentSetterConfig;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;

/**
 * An http client used to get data from given url. It follows the configurations from
 * {@link HTTPArgumentSetterConfig}
 */
public class HttpClient implements Closeable {

  private final Map<String, String> headers;
  private final HTTPArgumentSetterConfig config;
  private final StringEntity requestBody;
  private CloseableHttpClient httpClient;

  public HttpClient(HTTPArgumentSetterConfig config) {
    this.config = config;
    this.headers = config.getRequestHeadersMap();

    String requestBodyString = config.getBody();
    if (requestBodyString != null) {
      requestBody = new StringEntity(requestBodyString, Charsets.UTF_8.toString());
    } else {
      requestBody = null;
    }
  }

  /**
   * Executes HTTP request with parameters configured in plugin config and returns response. Is
   * called to load every page by pagination iterator.
   *
   * @param uri URI of resource
   * @return a response object
   * @throws IOException in case of a problem or the connection was aborted
   */
  public CloseableHttpResponse executeHTTP(String uri) throws IOException {
    // lazy init. So we are able to initialize the class for different checks during validations etc.
    if (httpClient == null) {
      httpClient = createHttpClient(uri);
    }

    HttpEntityEnclosingRequestBase request = new HttpRequest(URI.create(uri), config.getMethod());

    if (requestBody != null) {
      request.setEntity(requestBody);
    }

    // Set the Request Headers(along with Authorization Header) in the HttpRequest
    request.setHeaders(getRequestHeaders());

    return httpClient.execute(request);
  }

  @Override
  public void close() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  @VisibleForTesting
  public CloseableHttpClient createHttpClient(String pageUriStr) throws IOException {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();

    // set timeouts
    Long connectTimeoutMillis = TimeUnit.SECONDS.toMillis(config.getConnectTimeout());
    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder.setConnectTimeout(connectTimeoutMillis.intValue());
    requestBuilder.setConnectionRequestTimeout(connectTimeoutMillis.intValue());
    httpClientBuilder.setDefaultRequestConfig(requestBuilder.build());

    // basic auth
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    if (!Strings.isNullOrEmpty(config.getUsername()) && !Strings.isNullOrEmpty(
        config.getPassword())) {
      URI uri = URI.create(pageUriStr);
      AuthScope authScope = new AuthScope(
          new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()));
      credentialsProvider.setCredentials(authScope,
          new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
    }
    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);

    return httpClientBuilder.build();
  }

  private Header[] getRequestHeaders() throws IOException {
    ArrayList<Header> clientHeaders = new ArrayList<>();

    // set default headers
    if (headers != null) {
      for (Map.Entry<String, String> headerEntry : this.headers.entrySet()) {
        clientHeaders.add(new BasicHeader(headerEntry.getKey(), headerEntry.getValue()));
      }
    }

    return clientHeaders.toArray(new Header[0]);
  }

  /**
   * This class allows us to send body not only in POST/PUT but also in other requests.
   */
  private static class HttpRequest extends HttpEntityEnclosingRequestBase {

    private final String methodName;

    HttpRequest(URI uri, String methodName) {
      super();
      this.setURI(uri);
      this.methodName = methodName;
    }

    @Override
    public String getMethod() {
      return methodName;
    }
  }
}
