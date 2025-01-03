/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.policies.data;

public enum BrokerOperation {
    LIST_BROKERS,
    GET_BROKER,

    GET_LEADER_BROKER,
    LIST_OWNED_NAMESPACES,

    LIST_DYNAMIC_CONFIGURATIONS,
    UPDATE_DYNAMIC_CONFIGURATION,
    DELETE_DYNAMIC_CONFIGURATION,

    LIST_RUNTIME_CONFIGURATIONS,

    GET_INTERNAL_CONFIGURATION_DATA,

    CHECK_BACKLOG_QUOTA,
    HEALTH_CHECK,
    SHUTDOWN
}
