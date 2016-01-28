package com.googlecode.jmxtrans;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.googlecode.jmxtrans.cli.JmxTransConfiguration;
import com.googlecode.jmxtrans.monitoring.ManagedObject;
import com.googlecode.jmxtrans.monitoring.ManagedThreadPoolExecutor;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.TimeUnit.SECONDS;

public class JmxTransformerService extends AbstractIdleService {
	private static final Logger log = LoggerFactory.getLogger(JmxTransformerService.class);

	@Nonnull private final ThreadPoolExecutor queryProcessorExecutor;
	@Nonnull private final ThreadPoolExecutor resultProcessorExecutor;
	@Nonnull private final JmxTransConfiguration configuration;
	@Nonnull private final Scheduler serverScheduler;

	@Inject
	public JmxTransformerService(
			@Nonnull @Named("queryProcessorExecutor") ThreadPoolExecutor queryProcessorExecutor,
			@Nonnull @Named("resultProcessorExecutor") ThreadPoolExecutor resultProcessorExecutor,
			@Nonnull JmxTransConfiguration configuration,
			@Nonnull Scheduler serverScheduler) {
		this.queryProcessorExecutor = queryProcessorExecutor;
		this.resultProcessorExecutor = resultProcessorExecutor;
		this.configuration = configuration;
		this.serverScheduler = serverScheduler;
	}

	@Override
	protected void startUp() throws Exception {
		log.info("Starting Jmxtrans on : {}", configuration.getJsonDirOrFile());
		// TODO: register mbean for JmxTransformer

		registerExecutor(queryProcessorExecutor, "queryProcessorExecutor");
		registerExecutor(resultProcessorExecutor, "resultProcessorExecutor");

		serverScheduler.start();
	}

	private void registerExecutor(ThreadPoolExecutor executor, String name) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
		ManagedThreadPoolExecutor executorMBean = new ManagedThreadPoolExecutor(executor, name);
		ManagementFactory.getPlatformMBeanServer().registerMBean(executorMBean, executorMBean.getObjectName());
		addListener(new UnregisterMBeanListener(executorMBean), directExecutor());
	}

	@Override
	protected void shutDown() throws Exception {
		serverScheduler.shutdown(true);
		shutdownAndAwaitTermination(queryProcessorExecutor, 10, SECONDS);
		shutdownAndAwaitTermination(resultProcessorExecutor, 10, SECONDS);
	}

	private static final class UnregisterMBeanListener extends Listener {
		private static final Logger log = LoggerFactory.getLogger(UnregisterMBeanListener.class);

		private final ManagedObject managedObject;

		public UnregisterMBeanListener(ManagedObject managedObject) {
			this.managedObject = managedObject;
		}

		@Override
		public void terminated(State from) {
			try {
				ManagementFactory.getPlatformMBeanServer().unregisterMBean(managedObject.getObjectName());
			} catch (InstanceNotFoundException e) {
				log.error("Could not unregister {}.", managedObject, e);
			} catch (MBeanRegistrationException e) {
				log.error("Could not unregister {}.", managedObject, e);
			} catch (MalformedObjectNameException e) {
				log.error("Could not unregister {}.", managedObject, e);
			}
		}
	}
}
