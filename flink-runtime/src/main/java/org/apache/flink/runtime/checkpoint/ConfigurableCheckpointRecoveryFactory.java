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

import org.apache.flink.configuration.Configuration;

/**
 * A factory for per Job checkpoint recovery components that can be configured with a {@link Configuration}
 * when running Flink in a standalone mode.
 *
 * <p>A ConfigurableCheckpointRecoveryFactory is instantiated via reflection and must be public, non-abstract,
 * and have a public no-argument constructor.</p>
 */
public interface ConfigurableCheckpointRecoveryFactory extends CheckpointRecoveryFactory {

	/**
	 * Configures the {@link ConfigurableCheckpointRecoveryFactory} when running Flink in a standalone mode.
	 *
	 * @param config A {@link Configuration} object that contains all the parameters prefixed
	 *                  with "high-availability.standalone.checkpoint-recovery-factory"
	 */
	void configure(Configuration config);

}
