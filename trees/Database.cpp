#include "Database.hpp"

#include <iostream>
#include <ostream>

#include "BufferPool.hpp"

using namespace db;

void Database::add(std::unique_ptr<BaseFile> tree) {
    const std::string &name = tree->getName();
    std::cout << "[Database] add file: " << name << std::endl;
    files[name] = std::move(tree);
}

BaseFile &Database::get(const std::string &name) const {
    return *files.at(name);
}

BufferPool &Database::getBufferPool() {
    return buffer_pool;
}

Database &db::getDatabase() {
    static Database instance;
    return instance;
}

// std::unique_ptr<BaseFile> Database::remove(const std::string &name) {
//     auto nh = files.extract(name);
//     if (nh.empty()) {
//         // throw std::logic_error("File does not exist");
//         return nullptr;
//     }
//     Database::getBufferPool().flushFile(nh.key());
//     return std::move(nh.mapped());
// }
