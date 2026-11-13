#pragma once

#include <cstdint>

#include "cvm/cvm_types.h"

namespace mooncake {
namespace cvm {

// Callback interface implemented by the embedding master (or the HA
// supervisor bridging for it). CvmController calls into this delegate to
// notify role and membership changes without taking a dependency on the
// concrete MasterService type.
class CvmServiceDelegate {
   public:
    virtual ~CvmServiceDelegate() = default;

    // Called when this master becomes the stable primary owner of `slot`.
    virtual void OnSlotAcquired(uint16_t slot) = 0;

    // Called when this master releases ownership of `slot`.
    virtual void OnSlotReleased(uint16_t slot) = 0;

    // Called when the CVM membership coordinator decides this master's role
    // should change (e.g. demoted to standby because the submaster quota is
    // full, or promoted back to primary). MasterService reacts by switching
    // its serving/standby state machine accordingly.
    virtual void OnRoleChanged(MasterRole new_role) = 0;

    // Called when the CVM master member set changes (a master registered or
    // its lease expired). A standby re-derives its replay sources locally from
    // the updated member list (deterministic ring) even when its own role
    // stays kStandby — the replacement for the removed persisted kv_view.
    virtual void OnMembershipChanged() {}
};

}  // namespace cvm
}  // namespace mooncake
