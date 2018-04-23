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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class contains a utility method to load {@link ConfigurableCheckpointRecoveryFactory} from configurations.
 */
public class ConfigurableCheckpointRecoveryFactoryLoader {

	private static final Logger LOG = LoggerFactory.getLogger(ConfigurableCheckpointRecoveryFactoryLoader.class);

	/** The prefix for the {@link ConfigurableCheckpointRecoveryFactory}'s parameters. */
	@VisibleForTesting
	static final String HA_STANDALONE_CHECKPOINT_RECOVERY_FACTORY_PREFIX = "high-availability.standalone.checkpoint-recovery-factory";

	/**
	 * Loads a {@link ConfigurableCheckpointRecoveryFactory} from the configuration from the parameter
	 * 'high-availability.standalone.checkpoint-recovery-factory.class', as defined in
	 * {@link HighAvailabilityOptions#HA_STANDALONE_CHECKPOINT_RECOVERY_FACTORY_CLASS}
	 *
	 * <p>If no {@link ConfigurableCheckpointRecoveryFactory} is specified, it returns a
	 * {@link StandaloneCheckpointRecoveryFactory}.</p>
	 *
	 * <p>The {@link ConfigurableCheckpointRecoveryFactory} is instantiated (via its zero-argument constructor)
	 * and its {@link ConfigurableCheckpointRecoveryFactory#configure(Configuration)} method is called.</p>
	 *
	 * @param config The configuration to load the {@link ConfigurableCheckpointRecoveryFactory} from
	 *
	 * @return The configured {@link ConfigurableCheckpointRecoveryFactory}.
	 *
	 * @throws DynamicCodeLoadingException
	 * 				Thrown if a {@link ConfigurableCheckpointRecoveryFactory} is configured and the class was not
	 * 				found or it could not be instantiated
	 */
	public static ConfigurableCheckpointRecoveryFactory loadConfigurableCheckpointRecoveryFactoryFromConfig(
		Configuration config) throws DynamicCodeLoadingException {

		checkNotNull(config);

		final String factoryClassName = config.getString(
			HighAvailabilityOptions.HA_STANDALONE_CHECKPOINT_RECOVERY_FACTORY_CLASS);
		final Configuration factoryConfigs = new DelegatingConfiguration(
			config,
			HA_STANDALONE_CHECKPOINT_RECOVERY_FACTORY_PREFIX);
		if (factoryClassName == null) {
			LOG.info(
				String.format(
					"Configuration '%s' is empty; defaulting to StandaloneCheckpointRecoveryFactory",
					HighAvailabilityOptions.HA_STANDALONE_CHECKPOINT_RECOVERY_FACTORY_CLASS.key()));
			return new StandaloneCheckpointRecoveryFactory();
		} else {
			LOG.info(String.format("Loading ConfigurableCheckpointRecoveryFactory: %s", factoryClassName));
			try {
				final Class<?> clazz = Class.forName(factoryClassName);
				final ConfigurableCheckpointRecoveryFactory factory =
					(ConfigurableCheckpointRecoveryFactory) clazz.newInstance();
				LOG.info(String.format("Configuring ConfigurableCheckpointRecoveryFactory: %s", factoryClassName));
				factory.configure(factoryConfigs);
				return factory;
			} catch (ClassNotFoundException e) {
				throw new DynamicCodeLoadingException(
					String.format(
						"Cannot find configured ConfigurableCheckpointRecoveryFactory class: %s",
						factoryClassName), e);
			} catch (ClassCastException | InstantiationException | IllegalAccessException e) {
				throw new DynamicCodeLoadingException(
					String.format(
						"The class configured under '%s' is not a valid ConfigurableCheckpointRecoveryFactory (%s)",
						HighAvailabilityOptions.HA_STANDALONE_CHECKPOINT_RECOVERY_FACTORY_CLASS.key(),
						factoryClassName), e);
			}
		}
	}

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated */
	private ConfigurableCheckpointRecoveryFactoryLoader() {}

}
