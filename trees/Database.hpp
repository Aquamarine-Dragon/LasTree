#pragma once
#include <OptimizedBTree.hpp>

#include "BaseFile.hpp"


namespace db {
    class Database {
        std::unordered_map<std::string, std::unique_ptr<BaseFile>> files;

    public:
        void add(std::unique_ptr<BaseFile> tree) {
            const std::string &name = tree->getName();
            files[name] = std::move(tree);
        }

        BaseFile &get(const std::string &name) const {
            return *files.at(name);
        }

    };

    inline Database& getDatabase() {
        static Database instance;
        return instance;
    }
}


