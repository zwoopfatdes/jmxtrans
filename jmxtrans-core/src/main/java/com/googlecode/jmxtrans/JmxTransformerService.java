package com.googlecode.jmxtrans;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.googlecode.jmxtrans.cli.JmxTransConfiguration;
import com.googlecode.jmxtrans.exceptions.LifecycleException;
import com.googlecode.jmxtrans.jobs.ServerJob;
import com.googlecode.jmxtrans.model.OutputWriter;
import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Server;
import com.googlecode.jmxtrans.model.ValidationException;
import com.googlecode.jmxtrans.monitoring.ManagedObject;
import com.googlecode.jmxtrans.monitoring.ManagedThreadPoolExecutor;
import org.apache.commons.lang.RandomStringUtils;
import org.quartz.CronExpression;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.text.ParseException;
import java.util.Date;
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
	@Nonnull private final ConfigurationParser configurationParser;

	@Inject
	public JmxTransformerService(
			@Nonnull @Named("queryProcessorExecutor") ThreadPoolExecutor queryProcessorExecutor,
			@Nonnull @Named("resultProcessorExecutor") ThreadPoolExecutor resultProcessorExecutor,
			@Nonnull JmxTransConfiguration configuration,
			@Nonnull Scheduler serverScheduler,
			@Nonnull ConfigurationParser configurationParser) {
		this.queryProcessorExecutor = queryProcessorExecutor;
		this.resultProcessorExecutor = resultProcessorExecutor;
		this.configuration = configuration;
		this.serverScheduler = serverScheduler;
		this.configurationParser = configurationParser;
	}

	@Override
	protected void startUp() throws Exception {
		log.info("Starting Jmxtrans on : {}", configuration.getJsonDirOrFile());
		// TODO: register mbean for JmxTransformer

		scheduleAllServerJobs();

		registerExecutor(queryProcessorExecutor, "queryProcessorExecutor");
		registerExecutor(resultProcessorExecutor, "resultProcessorExecutor");

		serverScheduler.start();
	}

	private void scheduleAllServerJobs() throws LifecycleException {
		for (Server server : configurationParser.parseServers(getJsonFiles())) {
			try {
				for (Query query : server.getQueries()) {
					for (OutputWriter writer : query.getOutputWriterInstances()) {
						writer.start();
					}
				}

				// Now validate the setup of each of the OutputWriter's per
				// query.
				validateSetup(server, server.getQueries());

				// Now schedule the jobs for execution.
				this.scheduleJob(server);
			} catch (ParseException ex) {
				throw new LifecycleException("Error parsing cron expression: " + server.getCronExpression(), ex);
			} catch (SchedulerException ex) {
				throw new LifecycleException("Error scheduling job for server: " + server, ex);
			} catch (ValidationException ex) {
				throw new LifecycleException("Error validating json setup for query", ex);
			}
		}
	}

	private void validateSetup(Server server, ImmutableSet<Query> queries) throws ValidationException {
		for (Query q : queries) {
			for (OutputWriter w : q.getOutputWriterInstances()) {
				w.validateSetup(server, q);
			}
		}
	}

	private void scheduleJob(Server server) throws ParseException, SchedulerException {

		String name = server.getHost() + ":" + server.getPort() + "-" + System.currentTimeMillis() + "-" + RandomStringUtils.randomNumeric(10);
		JobDetail jd = new JobDetail(name, "ServerJob", ServerJob.class);

		JobDataMap map = new JobDataMap();
		map.put(Server.class.getName(), server);
		jd.setJobDataMap(map);

		Trigger trigger;

		if ((server.getCronExpression() != null) && CronExpression.isValidExpression(server.getCronExpression())) {
			trigger = new CronTrigger();
			((CronTrigger) trigger).setCronExpression(server.getCronExpression());
			trigger.setName(server.getHost() + ":" + server.getPort() + "-" + Long.toString(System.currentTimeMillis()));
			trigger.setStartTime(new Date());
		} else {
			int runPeriod = configuration.getRunPeriod();
			if (server.getRunPeriodSeconds() != null) runPeriod = server.getRunPeriodSeconds();

			Trigger minuteTrigger = TriggerUtils.makeSecondlyTrigger(runPeriod);
			minuteTrigger.setName(server.getHost() + ":" + server.getPort() + "-" + Long.toString(System.currentTimeMillis()));
			minuteTrigger.setStartTime(new Date());

			trigger = minuteTrigger;
		}

		serverScheduler.scheduleJob(jd, trigger);
		if (log.isDebugEnabled()) {
			log.debug("Scheduled job: " + jd.getName() + " for server: " + server);
		}
	}

	private Iterable<File> getJsonFiles() {
		return null;
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
