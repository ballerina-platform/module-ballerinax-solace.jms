// Copyright (c) 2026 WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerinax/solace;

isolated function validateConfigurations(ConnectionConfiguration config) returns Error? {
    // Validate compression level
    int compressionLevel = config.compressionLevel;
    if compressionLevel < 0 {
        return error Error("ZLIB compression level must be at least 0 (no compression)");
    }
    if compressionLevel > 9 {
        return error Error("ZLIB compression level cannot exceed 9 (maximum compression)");
    }

    // Validate auth configurations
    var authConfig = config.auth;
    if authConfig is solace:BasicAuthConfig {
        string username = authConfig.username;
        if username.length() > 32 {
            return error Error("Username cannot exceed 32 characters");
        }

        string? password = authConfig.password;
        if password is string && password.length() > 128 {
            return error Error("Password cannot exceed 128 characters");
        }
    }
}

isolated function validatePublisherConfigurations(PublisherConfiguration config) returns Error? {
    check validateConfigurations(config);

    BackPressureConfig backPressure = config.backPressure;
    if backPressure.strategy != ELASTIC && backPressure.bufferCapacity < 1 {
        return error Error("Back-pressure buffer capacity must be at least 1");
    }
}
