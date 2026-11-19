// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SERIALIZATION_H_
#define SERIALIZATION_H_

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace mooncake {

/**
 * @brief Convert a hexadecimal character to its integer value
 *
 * @param c Hexadecimal character ('0'-'9', 'A'-'F', 'a'-'f')
 * @return Integer value (0-15)
 * @throws std::invalid_argument if character is not a valid hex digit
 */
inline int hexCharToValue(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return 10 + c - 'A';
    if (c >= 'a' && c <= 'f') return 10 + c - 'a';
    throw std::invalid_argument("Invalid hexadecimal character");
}

/**
 * @brief Serialize binary data to hexadecimal string representation
 *
 * This function converts binary data (e.g., IPC memory handles) into a
 * hexadecimal string for transmission or storage.
 *
 * @param data Pointer to binary data
 * @param length Length of data in bytes
 * @return Hexadecimal string representation (2 characters per byte)
 * @throws std::invalid_argument if data pointer is null
 */
inline std::string serializeBinaryData(const void *data, size_t length) {
    if (!data) {
        throw std::invalid_argument("Data pointer cannot be null");
    }

    std::string hexString;
    hexString.reserve(length * 2);

    const auto *byteData = static_cast<const unsigned char *>(data);
    for (size_t i = 0; i < length; ++i) {
        hexString.push_back("0123456789ABCDEF"[(byteData[i] >> 4) & 0x0F]);
        hexString.push_back("0123456789ABCDEF"[byteData[i] & 0x0F]);
    }

    return hexString;
}

/**
 * @brief Deserialize hexadecimal string back to binary data
 *
 * This function converts a hexadecimal string back into binary data,
 * typically for reconstructing IPC memory handles.
 *
 * @param hexString Hexadecimal string to deserialize
 * @param buffer Output buffer to store binary data
 * @throws std::invalid_argument if input string length is not even
 */
inline void deserializeBinaryData(const std::string &hexString,
                                  std::vector<unsigned char> &buffer) {
    if (hexString.length() % 2 != 0) {
        throw std::invalid_argument("Input string length must be even");
    }

    buffer.clear();
    buffer.reserve(hexString.length() / 2);

    for (size_t i = 0; i < hexString.length(); i += 2) {
        int high = hexCharToValue(hexString[i]);
        int low = hexCharToValue(hexString[i + 1]);
        buffer.push_back(static_cast<unsigned char>((high << 4) | low));
    }
}

/**
 * @brief Convert a base64 character to its integer value
 *
 * @param c Base64 character (A-Z, a-z, 0-9, '+', '/')
 * @return Integer value (0-63)
 * @throws std::invalid_argument if character is not a valid base64 digit
 */
inline int base64CharToValue(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return 26 + c - 'a';
    if (c >= '0' && c <= '9') return 52 + c - '0';
    if (c == '+') return 62;
    if (c == '/') return 63;
    throw std::invalid_argument("Invalid base64 character");
}

/**
 * @brief Serialize binary data to a base64 string
 *
 * @param data Pointer to binary data
 * @param length Length of data in bytes
 * @return Base64 string representation
 * @throws std::invalid_argument if data pointer is null with non-zero length
 */
inline std::string base64Encode(const void *data, size_t length) {
    static constexpr char kAlphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    if (!data && length > 0) {
        throw std::invalid_argument("Data pointer cannot be null");
    }

    const auto *bytes = static_cast<const unsigned char *>(data);
    std::string encoded;
    encoded.reserve((length + 2) / 3 * 4);

    size_t i = 0;
    for (; i + 2 < length; i += 3) {
        uint32_t n = (static_cast<uint32_t>(bytes[i]) << 16) |
                     (static_cast<uint32_t>(bytes[i + 1]) << 8) |
                     static_cast<uint32_t>(bytes[i + 2]);
        encoded.push_back(kAlphabet[(n >> 18) & 0x3F]);
        encoded.push_back(kAlphabet[(n >> 12) & 0x3F]);
        encoded.push_back(kAlphabet[(n >> 6) & 0x3F]);
        encoded.push_back(kAlphabet[n & 0x3F]);
    }
    if (i < length) {
        uint32_t n = static_cast<uint32_t>(bytes[i]) << 16;
        if (i + 1 < length) n |= static_cast<uint32_t>(bytes[i + 1]) << 8;
        encoded.push_back(kAlphabet[(n >> 18) & 0x3F]);
        encoded.push_back(kAlphabet[(n >> 12) & 0x3F]);
        encoded.push_back(i + 1 < length ? kAlphabet[(n >> 6) & 0x3F] : '=');
        encoded.push_back('=');
    }
    return encoded;
}

/**
 * @brief Deserialize a base64 string back to binary data
 *
 * @param encoded Base64 string to deserialize
 * @return Decoded binary data
 * @throws std::invalid_argument if the input is not valid base64
 */
inline std::vector<unsigned char> base64Decode(const std::string &encoded) {
    if (encoded.length() % 4 != 0) {
        throw std::invalid_argument(
            "Input base64 length must be a multiple of 4");
    }

    std::vector<unsigned char> buffer;
    buffer.reserve(encoded.length() / 4 * 3);

    for (size_t i = 0; i < encoded.length(); i += 4) {
        uint32_t n = 0;
        int pad = 0;
        for (size_t j = 0; j < 4; ++j) {
            char c = encoded[i + j];
            if (c == '=') {
                if (j < 2) {
                    throw std::invalid_argument("Invalid base64 padding");
                }
                ++pad;
                n = n << 6;
                continue;
            }
            if (pad != 0) {
                throw std::invalid_argument("Invalid base64 padding");
            }
            n = (n << 6) | static_cast<uint32_t>(base64CharToValue(c));
        }
        buffer.push_back(static_cast<unsigned char>((n >> 16) & 0xFF));
        if (pad < 2)
            buffer.push_back(static_cast<unsigned char>((n >> 8) & 0xFF));
        if (pad < 1) buffer.push_back(static_cast<unsigned char>(n & 0xFF));
    }
    return buffer;
}

}  // namespace mooncake

#endif  // SERIALIZATION_H_
