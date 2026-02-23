FROM opensearchproject/opensearch:3.4.0

# Copy the plugin zip from your build directory
COPY build/distributions/tsdb-*.zip /tmp/

# Install the plugin
RUN /usr/share/opensearch/bin/opensearch-plugin install --batch file:///tmp/tsdb-3.4.0.0-SNAPSHOT.zip

# Clean up (ignore errors if file can't be removed)
RUN rm -f /tmp/tsdb-*.zip || true

# Expose OpenSearch ports
EXPOSE 9200 9300 9600
