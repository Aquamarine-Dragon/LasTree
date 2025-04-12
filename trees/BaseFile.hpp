#pragma once

#include <string>
#include <cstddef>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>


#include "Types.hpp"  // for db::Page

namespace db {

    /**
     * @brief BaseFile provides a common interface for all tree-based storage engines.
     * Any tree (e.g., BPlusTree, OptimizedBTree) should inherit from this.
     */
    class BaseFile {
    protected:
        int fd;  // file descriptor
        std::string filename;
    public:
        explicit BaseFile(const std::string& filename)
        : filename(filename) {
            fd = open(filename.c_str(), O_RDWR | O_CREAT, 0644);
            if (fd < 0) {
                throw std::runtime_error("Failed to open file: " + filename);
            }
        }

        virtual ~BaseFile() {
            if (fd >= 0) close(fd);
        }

        const std::string& getName() const {
            return filename;
        }

        virtual void readPage(Page& page, size_t id) const {
            std::fill(page.begin(), page.end(), 0);
            pread(fd, page.data(), DEFAULT_PAGE_SIZE, id * DEFAULT_PAGE_SIZE);
        }

        virtual void writePage(const Page& page, size_t id) const {
            pwrite(fd, page.data(), DEFAULT_PAGE_SIZE, id * DEFAULT_PAGE_SIZE);
        }

        virtual size_t getNumPages() const {
            off_t file_size = lseek(fd, 0, SEEK_END);
            return static_cast<size_t>(file_size) / DEFAULT_PAGE_SIZE;
        }
    };

} // namespace db
