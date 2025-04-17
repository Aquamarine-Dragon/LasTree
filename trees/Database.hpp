#pragma once

#include "BufferPool.hpp"
#include "BaseFile.hpp"

namespace db {
    class Database {
        std::unordered_map<std::string, std::unique_ptr<BaseFile>> files;

        BufferPool buffer_pool;

        Database() = default;

    public:

        friend Database &getDatabase();

        Database(Database const &) = delete;

        void operator=(Database const &) = delete;

        Database(Database &&) = delete;

        void operator=(Database &&) = delete;

        void add(std::unique_ptr<BaseFile> file);

        BufferPool &getBufferPool();

        BaseFile &get(const std::string &name) const;

        // std::unique_ptr<BaseFile> remove(const std::string &name);
    };

    Database& getDatabase();


}




