#pragma once

#include <cstdint>
#include <cstddef>

template <typename BaseHeader, typename PageHeader, typename SlotType, size_t MaxSlots, size_t BlockSize = 4096>
class PageLayout {
public:


    PageLayout(uint8_t* buffer)
        : buffer(buffer),
          base_header(reinterpret_cast<BaseHeader*>(buffer)),
          page_header(reinterpret_cast<PageHeader*>(buffer + sizeof(BaseHeader))),
          slots(reinterpret_cast<SlotType*>(buffer + sizeof(BaseHeader) + sizeof(PageHeader)))
    {}

    BaseHeader* base_header;
    PageHeader* page_header;
    SlotType* slots;


    uint8_t* metadata_end(size_t slot_count) const {
        return reinterpret_cast<uint8_t*>(slots + slot_count + 1);
    }

    size_t metadata_end_offset(size_t slot_count) const {
        return sizeof(BaseHeader)
             + sizeof(PageHeader)
             + sizeof(SlotType) * slot_count
             + sizeof(size_t); // for heap_end itself
    }


private:
    uint8_t* buffer;
};
