#pragma once
struct AllocatorTestLog {
    template <typename T>
    AllocatorTestLog& operator<<(const T&) { return *this; }
};
#define LOG(level) AllocatorTestLog{}
#define VLOG(level) AllocatorTestLog{}
