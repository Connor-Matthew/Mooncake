#ifndef MOONCAKE_SSD_REGISTER_CLIENT_H
#define MOONCAKE_SSD_REGISTER_CLIENT_H

#include "master_client.h"
#include "types.h"
#include <string>

namespace mooncake {
#define OPERATION_OK 0
#define OPERATION_FAILED -1

class NoFRegisterClient {
   public:
    NoFRegisterClient();
    ~NoFRegisterClient();

    /**
     * @brief Register a NoF SSD segment on the master
     * @param nqn NQN of the SSD
     * @param nsid Namespace ID
     * @param traddr Transport address
     * @param trsvcid Transport service ID
     * @param trtype Transport type ("RDMA" / "TCP" / "URMA"). Explicitly
     *        passing an invalid value fails the call; an empty string falls
     *        back to MC_NOF_TRTYPE then "RDMA" (legacy behavior).
     * @param base Base address of the SSD segment
     * @param size Size of the SSD segment
     * @param master_server_addr Master server address
     * @return int OPERATION_OK or OPERATION_FAILED
     */
    int set_register(const std::string &nqn, size_t nsid,
                     const std::string &traddr, size_t trsvcid,
                     const std::string &trtype, uintptr_t base, size_t size,
                     const std::string &master_server_addr);

    /**
     * @brief Unregister a NoF SSD segment by its te_endpoint
     * @param nqn NQN of the SSD
     * @param nsid Namespace ID
     * @param traddr Transport address
     * @param trsvcid Transport service ID
     * @param trtype Transport type ("RDMA" / "TCP" / "URMA"). Must match the
     *        value used at registration — the segment name is the full
     *        endpoint string.
     * @param master_server_addr Master server address
     * @return int OPERATION_OK or OPERATION_FAILED
     */
    int set_unregister_by_endpoint(const std::string &nqn, size_t nsid,
                                   const std::string &traddr, size_t trsvcid,
                                   const std::string &trtype,
                                   const std::string &master_server_addr);

   private:
    MasterClient master_client_;
};
}  // namespace mooncake

#endif  // MOONCAKE_SSD_REGISTER_CLIENT_H
