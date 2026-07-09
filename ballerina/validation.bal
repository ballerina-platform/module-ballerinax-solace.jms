// Copyright (c) 2025 WSO2 LLC. (http://www.wso2.com).
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

isolated function validateConfigurations(CommonConnectionConfiguration config) returns Error? {
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
    if authConfig is BasicAuthConfiguration {
        string username = authConfig.username;
        if username.length() > 32 {
            return error Error("Username cannot exceed 32 characters");
        }

        string? password = authConfig.password;
        if password is string && password.length() > 128 {
            return error Error("Password cannot exceed 128 characters");
        }
    }

    // Validate secure-socket configurations
    SecureSocket? secureSocket = config.secureSocket;
    if secureSocket is SecureSocket {
        string[]? trustedCommonNames = secureSocket.trustedCommonNames;
        if trustedCommonNames is string[] && trustedCommonNames.length() > 16 {
            return error Error("Trusted common names list cannot exceed 16 entries");
        }
    }

    // Validate flow-control configurations
    int? transportWindowSize = config.transportWindowSize;
    if transportWindowSize is int && (transportWindowSize < 1 || transportWindowSize > 255) {
        return error Error("transportWindowSize must be between 1 and 255");
    }
    int? ackThreshold = config.ackThreshold;
    if ackThreshold is int && (ackThreshold < 1 || ackThreshold > 75) {
        return error Error("ackThreshold must be between 1 and 75");
    }
    decimal? ackTimer = config.ackTimer;
    if ackTimer is decimal && (ackTimer < 0.02d || ackTimer > 1.5d) {
        return error Error("ackTimer must be between 0.02 and 1.5 seconds");
    }
    if config.directTransport && (transportWindowSize is int || ackThreshold is int || ackTimer is decimal) {
        return error Error(
                "transportWindowSize, ackThreshold, and ackTimer only apply when directTransport is false");
    }
}

