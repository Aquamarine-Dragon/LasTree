#pragma once

#include <cstdint>
#include <cstddef>

template <typename BaseHeader, typename PageHeader, typename SlotType, size_t MaxSlots, size_t BlockSize = 4096>
class PageLayout {
public:
    static constexpr size_t HEADER_SIZE =
        sizeof(BaseHeader) + sizeof(PageHeader) + sizeof(SlotType) * MaxSlots + sizeof(size_t);

    PageLayout(uint8_t* buffer)
        : buffer(buffer),
          base_header(reinterpret_cast<BaseHeader*>(buffer)),
          page_header(reinterpret_cast<PageHeader*>(buffer + sizeof(BaseHeader))),
          slots(reinterpret_cast<SlotType*>(buffer + sizeof(BaseHeader) + sizeof(PageHeader))),
          heap_end(reinterpret_cast<size_t*>(buffer + sizeof(BaseHeader) + sizeof(PageHeader) + sizeof(SlotType) * MaxSlots)),
          heap_base(buffer + BlockSize)
    {}

    BaseHeader* base_header;
    PageHeader* page_header;
    SlotType* slots;
    size_t* heap_end;
    uint8_t* heap_base;

    // Return pointer to beginning of tuple data area
    uint8_t* tuple_data_start() const {
        return reinterpret_cast<uint8_t*>(heap_end + 1);
    }

    // Remaining free space in page
    size_t free_space() const {
        return reinterpret_cast<size_t>(heap_end[0]) - (reinterpret_cast<size_t>(tuple_data_start()) - reinterpret_cast<size_t>(buffer));
    }



private:
    uint8_t* buffer;
};
