package org.elasticsearch.metrics.spark;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.spark.metrics.sink.Sink;
import org.elasticsearch.metrics.ElasticsearchReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;

/**
 * An implementation of {@link <a href="https://spark.apache.org/">Apache Sparks</a>} metric Sink 
 * to support Elasticsearch as a metrics destination.
 * 
 * @see Sink
 */
public class ElasticsearchSink implements Sink {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);
	
	public static final String ELASTICSEARCH_KEY_HOSTS = "hosts";
	public static final String ELASTICSEARCH_KEY_INDEX = "index";
	public static final String ELASTICSEARCH_KEY_PERIOD = "period";
	public static final String ELASTICSEARCH_KEY_UNIT = "unit";
	public static final String ELASTICSEARCH_KEY_PREFIX = "prefix";
	public static final String ELASTICSEARCH_KEY_ADD_FIELDS = "fields";
	public static final String ELASTICSEARCH_KEY_TIMESTAMP_FIELD = "tsfield";
	
	public static final String ELASTICSEARCH_INDEX_DEFAULT = "spark_metrics";
	public static final String ELASTICSEARCH_DEFAULT_PERIOD = "10";
	public static final String ELASTICSEARCH_DEFAULT_UNIT = "SECONDS";
	public static final String ELASTICSEARCH_DEFAULT_PREFIX = "";
	public static final String ELASTICSEARCH_DEFAULT_ADD_FIELDS = "";
	public static final String ELASTICSEARCH_DEFAULT_TIMESTAMP_FIELD = "timestamp";
	
	private final MetricRegistry initMetricsRegistry;
	private final int timePeriod;
	private final TimeUnit timeUnit;
	private final String[] esHosts;
	private final Map<String, String> additionalFields;
	private final String index;
	private final String prefix;
	private final String timestampField;
	
	private transient ElasticsearchReporter reporter;
	
	public ElasticsearchSink(final Properties properties, final MetricRegistry metricRegistry, final org.apache.spark.SecurityManager securityMgr) throws IOException {
		initMetricsRegistry = metricRegistry;
		
		final Props props = new Props(properties);
		
		final String hosts = props.get(ELASTICSEARCH_KEY_HOSTS).get();
		this.index = props.get(ELASTICSEARCH_KEY_INDEX).get();
		this.prefix = props.get(ELASTICSEARCH_KEY_PREFIX).get();
		this.timestampField = props.get(ELASTICSEARCH_KEY_TIMESTAMP_FIELD).or(ELASTICSEARCH_DEFAULT_TIMESTAMP_FIELD);
		final String addFields = props.get(ELASTICSEARCH_KEY_ADD_FIELDS).or(ELASTICSEARCH_DEFAULT_ADD_FIELDS);
		final String period = props.get(ELASTICSEARCH_KEY_PERIOD).or(ELASTICSEARCH_DEFAULT_PERIOD);
		final String unit = props.get(ELASTICSEARCH_KEY_UNIT).or(ELASTICSEARCH_DEFAULT_UNIT);
		

		final Splitter hostsSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
		final List<String> _hosts = hostsSplitter.splitToList(hosts);
		this.esHosts = _hosts.toArray(new String[_hosts.size()]);
		
		final MapSplitter fieldsSplitter = Splitter.on(',').omitEmptyStrings().trimResults().withKeyValueSeparator(':');
		this.additionalFields = fieldsSplitter.split(addFields);
		
		this.timePeriod = Integer.parseInt(period);
		this.timeUnit = TimeUnit.valueOf(unit);
	}
	
	private class Props {
		private final Properties properties;
		
		public Props(final Properties properties) {
			this.properties = properties;
		}
		public Optional<String> get(String property){
			return Optional.fromNullable(properties.getProperty(property));
		}
	}
	
	protected MetricRegistry registry() {
		MetricRegistry registry;
		try {
			final Class<?> cls = Class.forName("org.apache.beam.runners.spark.metrics.WithMetricsSupport");
			final Method meth = cls.getMethod("forRegistry", MetricRegistry.class);
			registry = (MetricRegistry) meth.invoke(null, this.initMetricsRegistry);
			
			LOG.info("Extended MetricsRegistry with Apache-Beam Metrics");
		} catch(Throwable e) {
			registry = this.initMetricsRegistry;
			LOG.info("Using initial MetricsRegistry");
		}
		
		return registry;
	}
	
	public ElasticsearchReporter createReporter(MetricRegistry registry) throws IOException {
		return ElasticsearchReporter.forRegistry(registry)
				.hosts(this.esHosts)
				.index(this.index)
				.prefixedWith(this.prefix)
				.timestampFieldname(this.timestampField)
				.additionalFields(this.additionalFields)
				.build();
	}
	
	protected ElasticsearchReporter reporter() throws IOException {
		if(reporter == null) {
			reporter = this.createReporter(registry());
		}
		return reporter;
	}

	@Override
	public void report() {
		if(reporter != null) {
			reporter.report();
		}
	}

	@Override
	public void start() {
		try {
			reporter().start(this.timePeriod, this.timeUnit);
		} catch (IOException e) {
			LOG.error("Failed to construct new reporter", e);
		}
	}

	@Override
	public void stop() {
		if(reporter != null) {
			reporter.stop();
			reporter = null;
		}
	}
}
