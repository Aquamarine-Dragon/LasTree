#pragma once

#include <vector>
#include <string>
#include <variant>
#include <cstdint>
#include <iostream>

#include "Types.hpp"

namespace db {
    class TupleDesc;

    class Tuple {
        std::vector<field_t> fields;
        // declared_types(i) specifies the type at fields(i)
        std::vector<type_t> declared_types;

    public:
        // empty tuple
        explicit Tuple(size_t field_count)
        : fields(field_count), declared_types(field_count, type_t::INT) {}

        Tuple(const std::vector<field_t> &fields, const std::vector<type_t> &types)
        : fields(fields), declared_types(types) {
            if (fields.size() != types.size()) {
                throw std::logic_error("Tuple: field count does not match type count");
            }
        }

        void set_field(size_t i, const field_t &value) {
            if (i >= fields.size()) {
                throw std::out_of_range("Tuple: index out of range in set_field");
            }
            fields[i] = value;
        }

        type_t field_type(size_t i) const {
            return declared_types.at(i);
        }

        size_t size() const {
            return fields.size();
        }

        const field_t &get_field(size_t i) const {
            return fields.at(i);
        }
    };

    class TupleDesc {
        std::vector<type_t> types;
        std::unordered_map<std::string, size_t> name_to_index;

    public:
        TupleDesc() = default;

        /**
         * @brief Construct a new Tuple Desc object
         * @details Construct a new TupleDesc object with the provided types and names
         * @param names the names of the fields
         * @param types the types of the fields
         * @throws std::logic_error if types and names have different lengths
         * @throws std::logic_error if names are not unique
         */
        TupleDesc(const std::vector<type_t> &types, const std::vector<std::string> &names) :types(types) {
            if (types.size() != names.size()) {
                throw std::logic_error("Types and names sizes do not match");
            }
            size_t offset = 0;
            for (size_t i = 0; i < types.size(); i++) {
                name_to_index[names[i]] = i;
            }
            if (name_to_index.size() != names.size()) {
                throw std::logic_error("Duplicate name");
            }
        }

        /**
         * @brief Check if the provided Tuple is compatible with this TupleDesc
         * @details A Tuple is compatible with a TupleDesc if the Tuple has the same number of fields and each field is of the
         * same type as the corresponding field in the TupleDesc
         * @param tuple the Tuple to check
         * @return true if the Tuple is compatible, false otherwise
         */
        bool compatible(const Tuple &tuple) const {
            if (tuple.size() != types.size()) {
                return false;
            }
            for (size_t i = 0; i < tuple.size(); i++) {
                if (tuple.field_type(i) != types[i]) {
                    return false;
                }
            }

            return true;
        }

        /**
         * @brief Get the index of the field
         * @details The index of the field is the position of the field in the Tuple
         * @param name the name of the field
         * @return the index of the field
         */
        size_t index_of(const std::string &name) const {
            return name_to_index.at(name);
        }

        /**
         * @brief Get the number of fields in the TupleDesc
         * @return the number of fields in the TupleDesc
         */
        size_t size() const {
            return types.size();
        }

        /**
         * @brief Get the length of the TupleDesc
         * @return the number of bytes needed to serialize a Tuple with this TupleDesc
         */
        size_t length(const Tuple& t) const {
            size_t len = 0;
            for (size_t i = 0; i < types.size(); ++i) {
                switch (types[i]) {
                    case type_t::INT: len += INT_SIZE; break;
                    case type_t::DOUBLE: len += DOUBLE_SIZE; break;
                    case type_t::CHAR: len += CHAR_SIZE; break;
                    case type_t::VARCHAR:
                        len += sizeof(uint16_t); // prefix length
                        len += std::get<std::string>(t.get_field(i)).size();
                    break;
                }
            }
            return len;
        }

        /**
         * @brief Serialize a Tuple
         * @param data the buffer to serialize the Tuple into
         * @param t the Tuple to serialize
         */
        void serialize(uint8_t *data, const Tuple &t) const {
            for (size_t i = 0; i < types.size(); i++) {
                const type_t &type = types[i];
                const field_t &field = t.get_field(i);
                switch (type) {
                    case type_t::INT:
                        *reinterpret_cast<int *>(data) = std::get<int>(field);
                    data += INT_SIZE;
                    break;
                    case type_t::DOUBLE:
                        *reinterpret_cast<double *>(data) = std::get<double>(field);
                    data += DOUBLE_SIZE;
                    break;
                    case type_t::CHAR: {
                        char *dst = reinterpret_cast<char *>(data);
                        const std::string &str = std::get<std::string>(field);
                        size_t len = std::min(str.size(), size_t(CHAR_SIZE - 1));
                        std::memcpy(dst, str.data(), len);
                        dst[len] = '\0';
                        std::memset(dst + len + 1, 0, CHAR_SIZE - len - 1);
                        data += CHAR_SIZE;
                        break;
                    }
                    case type_t::VARCHAR: {
                        const std::string &str = std::get<std::string>(field);
                        uint16_t len = str.size();
                        *reinterpret_cast<uint16_t *>(data) = len;
                        data += sizeof(uint16_t);
                        std::memcpy(data, str.data(), len);
                        data += len;
                        break;
                    }
                }
            }
        }

        /**
         * @brief Deserialize a Tuple
         * @param data the buffer to deserialize the Tuple from
         * @return the deserialized Tuple
         */
        Tuple deserialize(const uint8_t *data) const {
            std::vector<field_t> fields;
            fields.reserve(types.size());
            for (const type_t &type: types) {
                switch (type) {
                    case type_t::INT:
                        fields.emplace_back(*reinterpret_cast<const int *>(data));
                    data += INT_SIZE;
                    break;
                    case type_t::DOUBLE:
                        fields.emplace_back(*reinterpret_cast<const double *>(data));
                    data += DOUBLE_SIZE;
                    break;
                    case type_t::CHAR: {
                        fields.emplace_back(std::string(reinterpret_cast<const char *>(data), strnlen(reinterpret_cast<const char *>(data), CHAR_SIZE)));
                        data += CHAR_SIZE;
                        break;
                    }
                    case type_t::VARCHAR: {
                        uint16_t len = *reinterpret_cast<const uint16_t *>(data);
                        data += sizeof(uint16_t);
                        fields.emplace_back(std::string(reinterpret_cast<const char *>(data), len));
                        data += len;
                        break;
                    }
                }
            }
            return {fields, types};
        }

        /**
         * @brief Merge two TupleDescs
         * @details The merged TupleDesc has all the fields of the two TupleDescs
         * @param td1 the first TupleDesc
         * @param td2 the second TupleDesc
         * @return the merged TupleDesc
         */
        static db::TupleDesc merge(const TupleDesc &td1, const TupleDesc &td2) {
            std::vector<type_t> types(td1.types);
            types.insert(types.end(), td2.types.begin(), td2.types.end());
            std::vector<std::string> names(types.size());
            for (const auto &[name, index]: td1.name_to_index) {
                names[index] = name;
            }
            for (const auto &[name, index]: td2.name_to_index) {
                names[td1.size() + index] = name;
            }
            return {types, names};
        }
    };
} // namespace db

