/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.highavailability.nonha;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.ConfigurableCheckpointRecoveryFactoryLoader;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneRunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.StandaloneSubmittedJobGraphStore;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Abstract base class for non high-availability services.
 *
 * This class returns the standalone variants for the checkpoint recovery factory, the submitted
 * job graph store, the running jobs registry and the blob store.
 */
public abstract class AbstractNonHaServices implements HighAvailabilityServices {

	/** Logger for these services, shared with subclasses. */
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractNonHaServices.class);

	// ------------------------------------------------------------------------

	protected final Object lock = new Object();

	/** The Flink configuration of this component / process. */
	protected final Configuration config;

	private final RunningJobsRegistry runningJobsRegistry;

	private final VoidBlobStore voidBlobStore;

	private boolean shutdown;

	public AbstractNonHaServices(Configuration config) {
		this.config = checkNotNull(config);
		this.runningJobsRegistry = new StandaloneRunningJobsRegistry();
		this.voidBlobStore = new VoidBlobStore();

		shutdown = false;
	}

	// ----------------------------------------------------------------------
	// HighAvailabilityServices method implementations
	// ----------------------------------------------------------------------

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		synchronized (lock) {
			checkNotShutdown();

			try {
				return ConfigurableCheckpointRecoveryFactoryLoader.loadConfigurableCheckpointRecoveryFactoryFromConfig(config);
			} catch (Throwable e) {
				LOG.error("Failed to instantiate a ConfigurableCheckpointRecoveryFactory; " +
					"defaulting to StandaloneCheckpointRecoveryFactory.", e);
				return new StandaloneCheckpointRecoveryFactory();
			}
		}
	}

	@Override
	public SubmittedJobGraphStore getSubmittedJobGraphStore() throws Exception {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneSubmittedJobGraphStore();
		}
	}

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() throws Exception {
		synchronized (lock) {
			checkNotShutdown();

			return runningJobsRegistry;
		}
	}

	@Override
	public BlobStore createBlobStore() throws IOException {
		synchronized (lock) {
			checkNotShutdown();

			return voidBlobStore;
		}
	}

	@Override
	public void close() throws Exception {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;
			}
		}
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		// this stores no data, so this method is the same as 'close()'
		close();
	}

	// ----------------------------------------------------------------------
	// Helper methods
	// ----------------------------------------------------------------------

	@GuardedBy("lock")
	protected void checkNotShutdown() {
		checkState(!shutdown, "high availability services are shut down");
	}

	protected boolean isShutDown() {
		return shutdown;
	}
}
