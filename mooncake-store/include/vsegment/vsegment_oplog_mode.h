#pragma once

#include <cstdlib>
#include <cstring>

namespace mooncake::vsegment {

// vsegment oplog 模式开关（进程启动时读取一次，运行期不可切换）：
//   aligned （默认）：put 热路径只写 1 条 O(1) 的 VSEGMENT_RESERVE（+ 对象 PUT_END），
//                     分配器状态退化为纯内存，重启由 checkpoint + 对象元数据
//                     reconcile + RESERVE/RELEASE 重放共同重建。
//   full            ：旧行为 —— 每次 put 提交 2 条 O(N) 的 VSEGMENT_STATE，
//                     仅作为逃生开关：MC_VSEGMENT_OPLOG_MODE=full
//
// 两种模式写出的 oplog 语义不同，切换模式请从干净的 etcd 数据目录启动。
inline bool VSegmentOplogAlignedMode() {
    static const bool aligned = [] {
        const char* value = std::getenv("MC_VSEGMENT_OPLOG_MODE");
        if (value == nullptr || *value == '\0') {
            return true;  // 默认 aligned
        }
        return std::strcmp(value, "full") != 0;  // 只有显式 full 才关闭
    }();
    return aligned;
}

}  // namespace mooncake::vsegment
