#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>
#include <ylt/util/tl/expected.hpp>
#include <chrono>
#include <set>
#include <unordered_set>
#include <utility>

#include "client_metric.h"
#include "client_buffer.h"
#include "ha/leadership/leader_coordinator.h"
#include "master_client.h"
#include "storage_backend.h"
#include "thread_pool.h"
#include "transfer_engine.h"
#include "transfer_task.h"
#include "types.h"
#include "replica.h"
#include "master_metric_manager.h"
#include "count_min_sketch.h"
#include "local_hot_cache.h"
#include "pinned_buffer_pool.h"

namespace mooncake {

class PutOperation;
class DistributedStorageBackend;

std::optional<size_t> GetTransportRegistrationLimit(
    const std::string& protocol);

/**
 * @brief Result of a query operation containing replica information and lease
 * timeout
 */
class QueryResult {
   public:
    /** @brief List of available replicas for the queried key */
    const std::vector<Replica::Descriptor> replicas;
    /** @brief Time point when the lease for this key expires */
    const std::chrono::steady_clock::time_point lease_timeout;
    /** @brief Optional full-object checksum */
    const std::optional<uint64_t> object_checksum;
    /** @brief Tenant scope the master resolved the query to, when the master
     * reports it; readers forward it to disk-replica verify requests */
    const std::optional<std::string> resolved_tenant_id;
    /**
     * @brief Optional runtime layout table carried by the RPC response.
     * Only populated for vsegment replicas; shared across a batch.
     */
    std::shared_ptr<const std::vector<VSegmentLayoutEntry>> vsegment_layouts;

    QueryResult(
        std::vector<Replica::Descriptor>&& replicas_param,
        std::chrono::steady_clock::time_point lease_timeout_param,
        std::optional<uint64_t> object_checksum_param = std::nullopt,
        std::optional<std::string> resolved_tenant_id_param = std::nullopt)
        : replicas(std::move(replicas_param)),
          lease_timeout(lease_timeout_param),
          object_checksum(object_checksum_param),
          resolved_tenant_id(std::move(resolved_tenant_id_param)) {}

    bool IsLeaseExpired() const {
        return std::chrono::steady_clock::now() >= lease_timeout;
    }

    bool IsLeaseExpired(std::chrono::steady_clock::time_point& now) const {
        return now >= lease_timeout;
    }
};

/**
 * @brief Client for interacting with the mooncake distributed object store
 */
class Client {
   public:
    virtual ~Client();

    // One topology/registration snapshot serves every key or fragment in a
    // batch. Returned names remain owned by that snapshot.
    using HcaAffinity =
        std::function<const std::vector<std::string>&(std::span<const Slice>)>;
    static HcaAffinity MakeHcaAffinity(const Topology& topology,
                                       std::vector<SegmentBufferInfo> buffers);
    static std::optional<SegmentLocality> DiscoverSegmentLocality(
        const Topology& topology, const std::string& location);
    class ReadReplicaSelector {
       public:
        ErrorCode operator()(const std::vector<Replica::Descriptor>& replicas,
                             std::span<const Slice> destinations,
                             Replica::Descriptor& result);

       private:
        friend class Client;
        explicit ReadReplicaSelector(const Client* client) : client_(client) {}
        struct Context {
            HcaAffinity infer;
            std::unordered_set<std::string> endpoints;
        };
        const Client* client_;
        std::unique_ptr<Context> context_;
        bool initialized_{false};
    };
    ReadReplicaSelector MakeReadReplicaSelector() const {
        return ReadReplicaSelector(this);
    }

    using WriteBufferStager =
        std::function<tl::expected<std::vector<Slice>, ErrorCode>(
            const std::vector<Slice>&)>;

    // Optional diagnostic guard. Configure before starting reads; the callable
    // must support concurrent invocation. It sees the descriptor actually used
    // for transfer, including a possible local hot-cache redirection.
    using ReadReplicaValidator =
        std::function<bool(const std::string&, const Replica::Descriptor&)>;
    void SetReadReplicaValidator(ReadReplicaValidator validator) {
        read_replica_validator_ = std::move(validator);
    }

    const UUID& getClientId() const { return client_id_; }
    const std::string& tenant_id() const { return master_client_.tenant_id(); }
    void SetVSegmentTransferPlanner(
        std::shared_ptr<vsegment::VSegmentTransferPlanner> planner) {
        vsegment_transfer_planner_ = std::move(planner);
    }
    void SetReplicaTransferStagingAllocator(
        std::shared_ptr<ClientBufferAllocator> allocator) {
        replica_transfer_staging_allocator_ = std::move(allocator);
    }

    /**
     * @brief Creates and initializes a new Client instance
     * @param local_hostname Local host address (IP:Port)
     * @param metadata_connstring Connection string for metadata service
     * @param protocol Transfer protocol ("rdma" or "tcp")
     * @param device_names Comma-separated RDMA device names.
     *        Optional with default auto-discovery. Only required when
     *        auto-discovery is disabled (set env `MC_MS_AUTO_DISC=0`).
     * @param master_server_entry The entry of master server (IP:Port of master
     *        address for non-HA mode, or <backend>://connstring for HA mode,
     *        e.g. etcd://IP:Port;IP:Port;...;IP:Port)
     * @return std::optional containing a shared_ptr to Client if successful,
     * std::nullopt otherwise
     */
    static std::optional<std::shared_ptr<Client>> Create(
        const std::string& local_hostname,
        const std::string& metadata_connstring, const std::string& protocol,
        const std::optional<std::string>& device_names = std::nullopt,
        const std::string& master_server_entry = kDefaultMasterAddress,
        const std::shared_ptr<TransferEngine>& transfer_engine = nullptr,
        std::map<std::string, std::string> labels = {},
        const std::string& tenant_id = "default",
        std::shared_ptr<NVMeoFInitiator> nof_initiator = nullptr);

    // Test-only seam: when set, Client::Create builds the default NoF
    // initiator from this factory instead of CreateNofRuntime() whenever the
    // caller passes no initiator (nullptr). Not thread-safe; set before
    // Create and reset (pass nullptr) after the test.
    static void SetDefaultNofInitiatorFactoryForTesting(
        std::function<std::shared_ptr<NVMeoFInitiator>()> factory);

    // Test-only accessor for the wiring above: the initiator this client
    // ended up with (nullptr = NoF unavailable in this process).
    std::shared_ptr<NVMeoFInitiator> GetNofInitiatorForTesting() const;

    Status configureScheduling(
        const scheduling::SchedulerConfig& scheduler_config);

    /**
     * @brief Retrieves data for a given key
     * @param object_key Key to retrieve
     * @param slices Vector of slices to store the retrieved data
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Get(const std::string& object_key,
                                      std::vector<Slice>& slices);

    /**
     * @brief Batch retrieve data for multiple keys
     * @param object_keys Keys to query
     * @param slices Map of object keys to their data slices
     */
    std::vector<tl::expected<void, ErrorCode>> BatchGet(
        const std::vector<std::string>& object_keys,
        std::unordered_map<std::string, std::vector<Slice>>& slices);

    /**
     * @brief Batch query IP addresses for multiple client IDs.
     * @param client_ids Vector of client UUIDs to query.
     * @return An expected object containing a map from client_id to their IP
     * address lists on success, or an ErrorCode on failure.
     */
    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @return QueryResult containing replicas and lease timeout, or ErrorCode
     * indicating failure
     */
    tl::expected<QueryResult, ErrorCode> Query(const std::string& object_key);

    /**
     * @brief Read-only variant of Query(): fetches replica metadata without
     * granting a lease, triggering promotion-on-hit, or counting valid_get
     * metrics on the master.
     *
     * The returned QueryResult carries an already-expired lease
     * (lease_ttl_ms is forced to 0): callers must not start data transfers
     * based on it without a subsequent lease-granting Query().
     *
     * Intended for best-effort background paths (e.g. SSD prefetch metadata
     * classification) that must not perturb hot-path state.
     * @param object_key Key to query
     * @return QueryResult containing replicas, or ErrorCode indicating failure
     */
    tl::expected<QueryResult, ErrorCode> QueryReadOnly(
        const std::string& object_key);

    /**
     * @brief Queries replica lists for object keys that match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing a map from object keys to their
     * replica descriptors on success, or an ErrorCode on failure.
     */
    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    QueryByRegex(const std::string& str);

    /**
     * @brief Returns unique offload RPC endpoints currently known by master.
     */
    tl::expected<std::vector<std::string>, ErrorCode> GetOffloadEndpoints();

    /**
     * @brief Batch query object metadata without transferring data
     * @param object_keys Keys to query
     * @return Vector of QueryResult objects containing replicas and lease
     * timeouts
     */
    std::vector<tl::expected<QueryResult, ErrorCode>> BatchQuery(
        const std::vector<std::string>& object_keys);
    std::vector<tl::expected<QueryResult, ErrorCode>> BatchQuery(
        const std::vector<std::string>& object_keys,
        const std::string& tenant_id);

    /**
     * @brief Read-only variant of BatchQuery(); see QueryReadOnly() for the
     * lease/metrics semantics. Per-key results keep input order.
     */
    std::vector<tl::expected<QueryResult, ErrorCode>> BatchQueryReadOnly(
        const std::vector<std::string>& object_keys);
    std::vector<tl::expected<QueryResult, ErrorCode>> BatchQueryReadOnly(
        const std::vector<std::string>& object_keys,
        const std::string& tenant_id);

    tl::expected<void, ErrorCode> VerifyObjectChecksum(
        const std::string& object_key, const std::vector<Slice>& slices,
        size_t object_size, std::optional<uint64_t> expected_checksum);

    /**
     * @brief Batch clear KV cache for specified object keys on a specific
     * segment for a given client.
     * @param object_keys Vector of object key strings to clear.
     * @param client_id The UUID of the client that owns the object keys.
     * @param segment_name The name of the segment (storage device) to clear
     * from.
     * @return An expected object containing a vector of successfully cleared
     * object keys on success, or an ErrorCode on failure.
     */
    tl::expected<std::vector<std::string>, ErrorCode> BatchReplicaClear(
        const std::vector<std::string>& object_keys, const UUID& client_id,
        const std::string& segment_name);

    /**
     * @brief Transfers data using pre-queried object information
     * @param object_key Key of the object
     * @param query_result Previously queried object metadata containing
     * replicas and lease timeout
     * @param slices Vector of slices to store the data
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Get(const std::string& object_key,
                                      const QueryResult& query_result,
                                      std::vector<Slice>& slices);
    tl::expected<void, ErrorCode> Get(const std::string& object_key,
                                      const QueryResult& query_result,
                                      std::vector<Slice>& slices,
                                      uint64_t src_offset);
    std::optional<TransferEngine::ScatterTransferOperation> SubmitScatter(
        const std::vector<TransferEngine::ScatterTransferRange>& transfers,
        TransferIntent intent = TransferIntent::kUnspecified);

    std::optional<StoreScatterTransferOperation> SubmitScatterNative(
        const std::vector<TransferEngine::ScatterTransferRange>& transfers,
        TransferIntent intent = TransferIntent::kUnspecified);

    /**
     * @brief Transfers data using pre-queried object information
     * @param object_keys Keys of the objects
     * @param query_results Previously queried object metadata for each key
     * @param slices Map of object keys to their data slices
     * @param group_by_target Read exact-size memory replicas on the same
     * segment with one transfer submission. A failed submission or transfer
     * then fails every key in that group. A size mismatch stays per key.
     * Defaults to false.
     * @return Vector of ErrorCode results for each object
     */
    std::vector<tl::expected<void, ErrorCode>> BatchGet(
        const std::vector<std::string>& object_keys,
        const std::vector<QueryResult>& query_results,
        std::unordered_map<std::string, std::vector<Slice>>& slices,
        bool group_by_target = false,
        bool prefer_alloc_in_same_node = false);

    std::vector<tl::expected<void, ErrorCode>> BatchGetWhenPreferSameNode(
        const std::vector<std::string>& object_keys,
        const std::vector<QueryResult>& query_results,
        std::unordered_map<std::string, std::vector<Slice>>& slices);

    /**
     * @brief Stores data with replication
     * @param key Object key
     * @param slices Vector of data slices to store
     * @param config Replication configuration
     * @param enable_upsert_from_diagnostics Enable upsert_from-only SpDiag
     *        points and sampled breakdown logging
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                      std::vector<Slice>& slices,
                                      const ReplicateConfig& config);

    /**
     * @brief Batch put data with replication
     * @param keys Object keys
     * @param batched_slices Vector of vectors of data slices to store (indexed
     * to match keys)
     * @param config Replication configuration
     * @param group_by_target Write every memory replica on the same segment
     * with one transfer submission. A failed submission or transfer then
     * fails that replica for every key in the group. Defaults to false.
     * prefer_alloc_in_same_node also groups through this path.
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const ReplicateConfig& config);
    std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const ReplicateConfig& config, const WriteBufferStager& stager,
        bool group_by_target = false);

    /**
     * @brief Write slices into a memory replica at an object-byte offset.
     */
    ErrorCode TransferWriteRange(const Replica::Descriptor& replica_descriptor,
                                 std::vector<Slice>& slices,
                                 uint64_t dst_offset);

    /**
     * @brief Batch ranged read against cached replicas. Fragments from all
     * entries are issued as one scatter transfer so the transport can coalesce
     * everything bound for the same segment, then awaited together. Requires
     * memory replicas. Returns per-entry total bytes transferred or an
     * ErrorCode. Used by session get. No Master RPC.
     */
    std::vector<tl::expected<int64_t, ErrorCode>> BatchTransferReadRanges(
        const std::vector<Replica::Descriptor>& replicas,
        const std::vector<std::vector<Slice>>& slices,
        const std::vector<std::vector<uint64_t>>& src_offsets,
        TransferIntent intent = TransferIntent::kUnspecified,
        std::span<const QueryResult* const> query_results = {});

    /**
     * @brief Batch ranged read from restored LOCAL_DISK arenas.
     *
     * Counterpart of BatchTransferReadRanges the same way BatchGetOffloadObject
     * is the counterpart of BatchGet: one transfer_engine_addr for the batch,
     * plus a restore pointer and object size per entry. Fragments use
     * src_offsets into that restored object. Used by session get after RPC
     * restore into the owner's TE-registered client buffer. No Master RPC.
     */
    std::vector<tl::expected<int64_t, ErrorCode>>
    BatchTransferReadOffloadRanges(
        const std::string& transfer_engine_addr,
        const std::vector<uint64_t>& remote_bases,
        const std::vector<size_t>& remote_sizes,
        const std::vector<std::vector<Slice>>& slices,
        const std::vector<std::vector<uint64_t>>& src_offsets);

    /**
     * @brief Batch ranged write into cached replicas (replication). Fragments
     * from all entries and all memory replicas are issued as one scatter
     * transfer, then awaited together. Returns per-entry logical bytes
     * transferred (counted once, not per replica) or an ErrorCode. Used by
     * session put.
     */
    std::vector<tl::expected<int64_t, ErrorCode>> BatchTransferWriteRanges(
        const std::vector<std::vector<Replica::Descriptor>>& replicas_per_entry,
        const std::vector<std::vector<Slice>>& slices,
        const std::vector<std::vector<uint64_t>>& dst_offsets,
        TransferIntent intent = TransferIntent::kUnspecified);

    /**
     * @brief Upserts data: inserts if key doesn't exist, updates if it does
     * @param key Object key
     * @param slices Vector of data slices to store
     * @param config Replication configuration
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Upsert(
        const ObjectKey& key, std::vector<Slice>& slices,
        const ReplicateConfig& config,
        bool enable_upsert_from_diagnostics = false);

    /**
     * @brief Batch upsert data with replication
     * @param keys Object keys
     * @param batched_slices Vector of vectors of data slices
     * @param config Replication configuration
     * @param group_by_target Same as BatchPut. Defaults to false.
     */
    std::vector<tl::expected<void, ErrorCode>> BatchUpsert(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const ReplicateConfig& config);
    std::vector<tl::expected<void, ErrorCode>> BatchUpsert(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const ReplicateConfig& config, const WriteBufferStager& stager,
        bool group_by_target = false);

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @param force If true, skip lease and replication task checks
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Remove(const ObjectKey& key,
                                         bool force = false);

    /**
     * @brief Removes objects from the store whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @param force If true, skip lease and replication task checks
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str,
                                                bool force = false);

    /**
     * @brief Removes all objects and all its replicas
     * @param force If true, skip lease and replication task checks
     * @return tl::expected<long, ErrorCode> number of removed objects or error
     */
    tl::expected<long, ErrorCode> RemoveAll(bool force = false);

    /**
     * @brief Batch remove objects and all their replicas
     * @param keys List of keys to remove
     * @param force If true, skip lease and replication task checks
     * @return Vector of expected results for each key
     */
    std::vector<tl::expected<void, ErrorCode>> BatchRemove(
        const std::vector<ObjectKey>& keys, bool force = false);

    /**
     * @brief Notify master that a disk replica was evicted locally
     * @param key The evicted object key
     * @param replica_type DISK or LOCAL_DISK
     */
    tl::expected<void, ErrorCode> EvictDiskReplica(const std::string& key,
                                                   ReplicaType replica_type);
    tl::expected<void, ErrorCode> EvictDiskReplica(const std::string& key,
                                                   const std::string& tenant_id,
                                                   ReplicaType replica_type);

    // Conditional variant: the master evicts only when its current record is
    // still the replica the caller failed on. See MasterClient for the
    // mixed-version behavior.
    tl::expected<bool, ErrorCode> EvictDiskReplicaIfCurrent(
        const std::string& key, const std::string& tenant_id,
        ReplicaType replica_type, ReplicaID expected_replica_id);

    std::vector<tl::expected<void, ErrorCode>> BatchEvictDiskReplica(
        const std::vector<std::string>& keys, ReplicaType replica_type);
    std::vector<tl::expected<void, ErrorCode>> BatchEvictDiskReplica(
        const std::vector<std::string>& keys, const std::string& tenant_id,
        ReplicaType replica_type);

    /**
     * @brief Registers a memory segment to master for allocation
     * @param buffer Memory buffer to register
     * @param size Size of the buffer in bytes
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> MountSegment(
        const void* buffer, size_t size, const std::string& protocol = "tcp",
        const std::string& location = kWildcardLocation);

    /**
     * @brief Unregisters a memory segment from master
     * @param buffer Memory buffer to unregister
     * @param size Size of the buffer in bytes
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                 size_t size);

    /**
     * @brief Mounts a memory segment and returns its generated Segment UUID.
     *        Logic is identical to MountSegment, but returns the segment id.
     */
    tl::expected<UUID, ErrorCode> MountSegmentAndGetId(
        const void* buffer, size_t size, const std::string& protocol = "tcp",
        const std::string& location = kWildcardLocation);

    /**
     * @brief Unmounts a segment by its UUID.
     *        Logic is identical to UnmountSegment, but looks up by id.
     * @param grace_period_ms 0 = immediate unmount (legacy behavior).
     */
    tl::expected<void, ErrorCode> UnmountSegmentById(
        const UUID& segment_id, uint64_t grace_period_ms = 0,
        std::function<void(const UUID&)> cleanup_callback = {});

    /**
     * @brief Registers memory buffer with TransferEngine for data transfer
     * @param addr Memory address to register
     * @param length Size of the memory region
     * @param location Device location (e.g. "cpu:0")
     * @param remote_accessible Whether the memory can be accessed remotely
     * @param update_metadata Whether to update metadata service
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> RegisterLocalMemory(
        void* addr, size_t length, const std::string& location,
        bool remote_accessible = true, bool update_metadata = true);

    /**
     * @brief Unregisters memory buffer from TransferEngine
     * @param addr Memory address to unregister
     * @param update_metadata Whether to update metadata service
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> unregisterLocalMemory(
        void* addr, bool update_metadata = true);

    /**
     * @brief Checks if an object exists
     * @param key Key to check
     * @return ErrorCode::OK if exists, ErrorCode::OBJECT_NOT_FOUND if not
     * exists, other ErrorCode for errors
     */
    tl::expected<bool, ErrorCode> IsExist(const std::string& key);

    /**
     * @brief Point-in-time existence check that grants no read lease.
     *        A `true` result only means the object existed at the time of
     *        the call; it may be evicted before a subsequent Get.
     * @param key Key to check
     * @return Vector of existence results for each key
     */
    tl::expected<bool, ErrorCode> ProbeKey(const std::string& key);

    /**
     * @brief Checks if multiple objects exist
     * @param keys Vector of keys to check
     * @return Vector of existence results for each key
     */
    std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys);

    WeightRpcResult<WeightRevisionMetadata> BeginWeightImport(
        const BeginWeightImportRequest& request);
    WeightRpcResult<WeightRevisionMetadata> CommitWeightImport(
        const CommitWeightImportRequest& request);
    WeightRpcResult<WeightRevisionMetadata> AbortWeightImport(
        const AbortWeightImportRequest& request);
    WeightRpcResult<WeightRevisionView> GetWeightRevision(
        const GetWeightRevisionRequest& request);
    WeightRpcResult<ListWeightRevisionsResponse> ListWeightRevisions(
        const ListWeightRevisionsRequest& request);
    WeightRpcResult<WeightRevisionLease> AcquireWeightRevisionLease(
        const AcquireWeightRevisionLeaseRequest& request);
    WeightRpcResult<WeightRevisionLease> RenewWeightRevisionLease(
        const RenewWeightRevisionLeaseRequest& request);
    WeightRpcResult<void> ReleaseWeightRevisionLease(
        const ReleaseWeightRevisionLeaseRequest& request);
    WeightRpcResult<WeightResidencyOperation> StartWeightResidencyOperation(
        const StartWeightResidencyOperationRequest& request);
    WeightRpcResult<WeightResidencyOperation> QueryWeightOperation(
        const QueryWeightOperationRequest& request);
    WeightRpcResult<WeightRevisionMetadata> DeleteWeightRevision(
        const DeleteWeightRevisionRequest& request);

    /**
     * @brief Probe multiple objects, optionally leasing the last complete
     *        candidate. LastHitOnly leases the last all-true candidate.
     * @param keys Vector of keys to check
     * @return Per-key existence results in input order
     */
    std::vector<tl::expected<bool, ErrorCode>> BatchProbeKey(
        const std::vector<std::string>& keys,
        const GrantLeasePolicy& policy = {});

    /**
     * @brief Create a copy task to copy an object's replicas to target segments
     * @param key Object key
     * @param targets Target segments
     * @return tl::expected<UUID, ErrorCode> Task ID on success, ErrorCode on
     * failure
     */
    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::vector<std::string>& targets);
    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::string& tenant_id,
        const std::vector<std::string>& targets);

    /**
     * @brief Create a move task to move an object's replica from source segment
     * to target segment
     * @param key Object key
     * @param source Source segment
     * @param target Target segment
     * @return tl::expected<UUID, ErrorCode> Task ID on success, ErrorCode on
     * failure
     */
    tl::expected<UUID, ErrorCode> CreateMoveTask(const std::string& key,
                                                 const std::string& source,
                                                 const std::string& target);
    tl::expected<UUID, ErrorCode> CreateMoveTask(const std::string& key,
                                                 const std::string& tenant_id,
                                                 const std::string& source,
                                                 const std::string& target);

    /**
     * @brief Query a task by task id
     * @param task_id Task ID to query
     * @return tl::expected<QueryTaskResponse, ErrorCode> Task basic info
     * on success, ErrorCode on failure
     */
    tl::expected<QueryTaskResponse, ErrorCode> QueryTask(const UUID& task_id);

    /**
     * @brief Get global segment base address for cxl protocol
     * @return Global segment base address
     */
    void* GetBaseAddr();

    /**
     * @brief Mounts a local disk segment into the master.
     * @param enable_offloading If true, enables offloading (write-to-file).
     */
    tl::expected<void, ErrorCode> MountLocalDiskSegment(bool enable_offloading);

    /**
     * @brief Deregisters this client's local disk segment from the master.
     * Idempotent. The master drops the LOCAL_DISK replicas this client owns, so
     * readers stop being handed a disk whose owner is about to stop serving.
     * Callers must stop offloading first, otherwise the storage heartbeat
     * re-mounts the segment on its next tick.
     */
    tl::expected<void, ErrorCode> UnmountLocalDiskSegment();

    /**
     * @brief Installs the existence probe used by the dangling-replica heal.
     * The probe lives outside this class because the offload file storage is
     * owned by the caller (RealClient), not by Client.
     */
    void SetLocalDiskProbe(
        std::function<std::optional<bool>(const std::string& key)> probe) {
        local_disk_probe_fn_ = std::move(probe);
    }

    /**
     * @brief Heartbeat call to collect object-level statistics and retrieve the
     * set of non-offloaded objects.
     * @param enable_offloading Indicates whether offloading is enabled for this
     * segment.
     * @param offloading_objects On return, contains the tenant-scoped object
     * tasks that require offload.
     */
    tl::expected<void, ErrorCode> OffloadObjectHeartbeat(
        bool enable_offloading,
        std::vector<OffloadTaskItem>& offloading_objects);

    tl::expected<bool, ErrorCode> PollRemoveAll();

    tl::expected<void, ErrorCode> ReportSsdCapacity(
        int64_t ssd_total_capacity_bytes);

    /**
     * @brief Heartbeat-driven pull of pending L2->L1 promotion work for this
     * client. Mirror of OffloadObjectHeartbeat. Returns tenant-scoped tasks the
     * caller (FileStorage) must read from local SSD and stage as MEMORY
     * replicas via PromotionAllocStart + NotifyPromotionSuccess.
     */
    // Virtual to enable subclassing in unit tests.
    virtual tl::expected<void, ErrorCode> PromotionObjectHeartbeat(
        std::vector<PromotionTaskItem>& promotion_objects);

    /**
     * @brief Register a master-side promotion task for SSD prefetch, without
     * the promotion-on-hit admission gate or heartbeat mailbox. This client
     * must hold the key's LOCAL_DISK replica. Best-effort:
     * PROMOTION_ALREADY_EXISTS means skip quietly. The client's tenant
     * applies.
     */
    virtual tl::expected<void, ErrorCode> RegisterPrefetchTask(
        const std::string& object_key, const std::string& tenant_id);
    /**
     * @brief Fetch pending removed keys for this client's LOCAL_DISK replicas.
     * The caller marks each key removed and acknowledges the processed tasks.
     */
    [[nodiscard]] tl::expected<std::vector<RemoveTaskItem>, ErrorCode>
    RemoveObjectHeartbeat(const UUID& client_id);

    tl::expected<void, ErrorCode> AckRemoveObjectHeartbeat(
        const UUID& client_id, const std::vector<RemoveTaskItem>& tasks);

    /**
     * @brief Stage a PROCESSING MEMORY replica for an existing key during
     * L2->L1 promotion. Returns the new replica's descriptor that the caller
     * writes via Transfer Engine before calling NotifyPromotionSuccess.
     */
    virtual tl::expected<PromotionAllocStartResponse, ErrorCode>
    PromotionAllocStart(const std::string& key, uint64_t size,
                        const std::vector<std::string>& preferred_segments);
    virtual tl::expected<PromotionAllocStartResponse, ErrorCode>
    PromotionAllocStart(const std::string& key, const std::string& tenant_id,
                        uint64_t size,
                        const std::vector<std::string>& preferred_segments);

    /**
     * @brief Commit a staged MEMORY replica to COMPLETE; called after the
     * client has written the bytes via Transfer Engine.
     */
    virtual tl::expected<void, ErrorCode> NotifyPromotionSuccess(
        const std::string& key);
    virtual tl::expected<void, ErrorCode> NotifyPromotionSuccess(
        const std::string& key, const std::string& tenant_id);

    /**
     * @brief Release master-side promotion task after a client-side failure
     * between PromotionAllocStart and the transfer's completion. Idempotent.
     */
    virtual tl::expected<void, ErrorCode> NotifyPromotionFailure(
        const std::string& key);
    virtual tl::expected<void, ErrorCode> NotifyPromotionFailure(
        const std::string& key, const std::string& tenant_id);

    /**
     * @brief Write `slices` into the memory replica described by
     * `memory_descriptor` via Transfer Engine. Used by FileStorage to fill a
     * PROCESSING memory replica staged by PromotionAllocStart before calling
     * NotifyPromotionSuccess.
     */
    virtual ErrorCode PromotionWrite(
        const Replica::Descriptor& memory_descriptor,
        std::vector<Slice>& slices);

    /**
     * @brief Performs a batched read of multiple objects using a
     * high-throughput Transfer Engine.
     */
    tl::expected<void, ErrorCode> BatchGetOffloadObject(
        const std::string& transfer_engine_addr,
        const std::vector<std::string>& keys,
        const std::vector<uintptr_t>& pointers,
        const std::unordered_map<std::string, std::vector<Slice>>&
            batch_slices);

    tl::expected<void, ErrorCode> BatchGetOffloadObject(
        const std::string& transfer_engine_addr,
        const std::vector<std::string>& keys,
        const std::vector<uintptr_t>& pointers,
        const std::unordered_map<std::string, std::vector<Slice>>& batch_slices,
        OffloadBufferAccess buffer_access,
        TransferIntent intent = TransferIntent::kForegroundGet);

    /**
     * @brief Push counterpart of BatchGetOffloadObject, run on the data owner.
     * WRITEs each key's staged ClientBuffer blob (src_pointers[i]) directly
     * into the requester's destination slices over the transfer engine.
     * @param requester_te_addr The requester's Transfer Engine endpoint.
     * @param keys List of keys (one per src pointer / dst slice group).
     * @param src_pointers Owner-local ClientBuffer addresses, one per key.
     * @param dst_slices Per-key destination slices on the requester.
     */
    tl::expected<void, ErrorCode> BatchPushOffloadObject(
        const std::string& requester_te_addr,
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>& src_pointers,
        const std::vector<std::vector<OffloadDstSlice>>& dst_slices);

    /**
     * @brief Notifies the master that offloading of specified objects has
     * succeeded.
     * @param keys         A list of object keys (names) that were successfully
     * offloaded.
     * @param metadatas    The corresponding metadata for each offloaded object,
     * including size, storage location, etc.
     */
    tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas);
    tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const std::vector<OffloadTaskItem>& tasks,
        const std::vector<StorageObjectMetadata>& metadatas);
    void SetDfsStorageBackend(
        std::shared_ptr<DistributedStorageBackend> backend);

    // === 多 submaster 定向 offload 控制面（FileStorage 专用） ===
    // 以下方法与上面的非定向版本对称：多 submaster 下 LOCAL_DISK 段与
    // offload 任务队列按 slot 分散在各 submaster，需逐 master 定向操作
    // （不切换业务"当前地址"）。单 master 模式（路由表为空）调用方回退
    // 非定向版本，GetSubmasterAddresses 返回空 vector 作为回退判定。

    /** 枚举所有 primary submaster 地址；空 = 单 master 模式。 */
    [[nodiscard]] std::vector<std::string> GetSubmasterAddresses() const;

    /** 定向 MountLocalDiskSegment：向指定 submaster 注册 LOCAL_DISK 段。 */
    virtual tl::expected<void, ErrorCode> MountLocalDiskSegmentTo(
        const std::string& address, bool enable_offloading);

    /**
     * @brief 定向卸载本 worker 的 LOCAL_DISK 段：向指定 submaster 清理
     * LocalDiskSegment（容量记账、offloading 队列），与 MountLocalDiskSegmentTo
     * 对称。submaster 移除时由 RefreshSubmasterAddresses removed 分支调用。
     */
    virtual tl::expected<void, ErrorCode> UnmountLocalDiskSegmentTo(
        const std::string& address);

    /** 定向 OffloadObjectHeartbeat：拉取指定 submaster 的卸载任务。 */
    tl::expected<void, ErrorCode> OffloadObjectHeartbeatTo(
        const std::string& address, bool enable_offloading,
        std::vector<OffloadTaskItem>& offloading_objects);

    /** 定向 ReportSsdCapacity：向指定 submaster 上报 SSD 容量。 */
    tl::expected<void, ErrorCode> ReportSsdCapacityTo(
        const std::string& address, int64_t ssd_total_capacity_bytes);

    /** 定向 PollRemoveAll：查询指定 submaster 是否要求全量清空 SSD。 */
    tl::expected<bool, ErrorCode> PollRemoveAllTo(const std::string& address);

    /** 定向拉取/确认指定 submaster 的 SSD tombstone 任务。 */
    [[nodiscard]] tl::expected<std::vector<RemoveTaskItem>, ErrorCode>
    RemoveObjectHeartbeatTo(const std::string& address);
    tl::expected<void, ErrorCode> AckRemoveObjectHeartbeatTo(
        const std::string& address, const std::vector<RemoveTaskItem>& tasks);

    /**
     * @brief 定向 promotion 执行链（任务状态在 source master 内存中，
     * AllocStart/Success/Failure 必须回到派发任务的 master 闭环）。
     * addr 为空时回退非定向调用（单 master 模式）。
     */
    virtual tl::expected<void, ErrorCode> PromotionObjectHeartbeatTo(
        const std::string& address,
        std::vector<PromotionTaskItem>& promotion_objects);
    virtual tl::expected<PromotionAllocStartResponse, ErrorCode>
    PromotionAllocStartTo(const std::string& address, const std::string& key,
                          const std::string& tenant_id, uint64_t size,
                          const std::vector<std::string>& preferred_segments);
    virtual tl::expected<void, ErrorCode> NotifyPromotionSuccessTo(
        const std::string& address, const std::string& key,
        const std::string& tenant_id);
    virtual tl::expected<void, ErrorCode> NotifyPromotionFailureTo(
        const std::string& address, const std::string& key,
        const std::string& tenant_id);

    /**
     * @brief 带来源（source master）的 offload 任务。source_address 为
     * 派发该任务的 submaster 地址（Heartbeat 拉取时记录）；空表示来源
     * 未知（本地 ScanMeta 重注册）或单 master 模式。双投清理协议依赖
     * 该信息：offload 完成时若 slot 已迁移到新 owner，任务状态（源副本
     * refcnt pin + offloading_tasks 条目）仍留在 source master，需要
     * 额外向 source 发一次"仅清理"通知。
     */
    struct SourcedOffloadTask {
        OffloadTaskItem task;
        std::string source_address;
    };

    /**
     * @brief 按 task 的 slot owner 分组定向上报卸载结果（双投清理协议）。
     * 多 submaster 模式下逐 task 解析 owner（task 自带 tenant）后分组：
     * - 成功通知投 owner（建副本）；若 source 非空且 != owner，双投
     *   source 一份（master 侧发现非本机 slot 但任务在本机时仅清理
     *   任务状态，不触碰元数据）。
     * - NACK（data_size<0）只投 source（清理语义）；source 空则回退
     *   当前连接。
     * - owner 未解析时投 source（source 侧 slot 校验决定建副本或清理）。
     * 单 master 模式等价于 NotifyOffloadSuccess。
     */
    tl::expected<void, ErrorCode> NotifyOffloadSuccessRouted(
        const std::vector<SourcedOffloadTask>& tasks,
        const std::vector<StorageObjectMetadata>& metadatas);

    /** 旧签名重载：无来源信息的通知（ScanMeta 重注册等），source 为空。 */
    tl::expected<void, ErrorCode> NotifyOffloadSuccessRouted(
        const std::vector<OffloadTaskItem>& tasks,
        const std::vector<StorageObjectMetadata>& metadatas);

    /**
     * @brief Fetch tasks assigned to a client
     * @param batch_size Number of tasks to fetch
     * @return tl::expected<std::vector<TaskAssignment>, ErrorCode> list of
     * tasks on success, ErrorCode on failure
     */
    tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        size_t batch_size);

    /**
     * @brief Mark the task as complete
     * @param task_complete Task complete request
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    tl::expected<void, ErrorCode> MarkTaskToComplete(
        const TaskCompleteRequest& task_complete);

    // For human-readable metrics
    tl::expected<std::string, ErrorCode> GetSummaryMetrics() {
        if (metrics_ == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return metrics_->summary_metrics();
    }

    tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats() {
        return master_client_.CalcCacheStats();
    }

    void ObserveTransferOperation(TransferOperationKind kind,
                                  const std::string& op_name, uint64_t bytes,
                                  uint64_t latency_us) {
        if (metrics_ != nullptr) {
            metrics_->ObserveTransferOperation(kind, op_name, bytes,
                                               latency_us);
        }
    }

    // For Prometheus-style metrics
    tl::expected<std::string, ErrorCode> SerializeMetrics() {
        if (metrics_ == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        std::string str;
        metrics_->serialize(str);
        return str;
    }

    SsdMetric* GetSsdMetricPtr() {
        return metrics_ ? &metrics_->ssd_metric : nullptr;
    }

    DfsMetric* GetDfsMetricPtr() {
        return metrics_ ? &metrics_->dfs_metric : nullptr;
    }

    [[nodiscard]] std::string GetTransportEndpoint() {
        return transfer_engine_->getLocalIpAndPort();
    }

    [[nodiscard]] const std::string& GetProtocol() const { return protocol_; }

    [[nodiscard]] bool HasClassicTransport(const std::string& protocol) const {
        return transfer_engine_ && !transfer_engine_->isUsingTent() &&
               transfer_engine_->getTransport(protocol) != nullptr;
    }

    [[nodiscard]] bool CanUseLocalMemcpy(const std::string& endpoint) const {
        return transfer_submitter_ != nullptr &&
               transfer_submitter_->canUseLocalMemcpy(endpoint);
    }
    // Inspect the actual remote transport; allocation descriptors carry a
    // storage tag rather than the Transfer Engine segment's protocol.
    [[nodiscard]] bool IsUbTransportSegment(const std::string& endpoint);

    /**
     * @brief Warmup transport-level connections to all segments
     * currently registered with the master.
     *
     * Allocates a buffer via the given ClientBufferAllocator (whose base is
     * already registered with the transfer engine) and issues a 1-byte WRITE
     * then READ per segment to trigger eager connection setup (e.g. RDMA QP
     * handshakes). This amortises the first-transfer handshake latency and
     * mitigates TRANSFER_FAIL caused by handshake storms under
     * high-concurrency small-file workloads.
     *
     * @param allocator Client buffer allocator used to allocate/release the
     *                 warmup buffer; its memory is already registered with
     *                 the transfer engine.
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> warmup(
        const std::shared_ptr<ClientBufferAllocator>& allocator);

    /**
     * @brief Get the endpoint address for segment operations.
     * @return For P2PHANDSHAKE mode, returns the actual RPC endpoint (IP:Port).
     *         For other modes, returns the logical local hostname used for
     * segment registration.
     */
    [[nodiscard]] std::string GetSegmentEndpoint() {
        return (metadata_connstring_ == P2PHANDSHAKE) ? GetTransportEndpoint()
                                                      : local_hostname_;
    }

    // Return sorted NUMA node IDs that have at least one RDMA NIC.
    [[nodiscard]] std::vector<int> GetNicNumaNodes() const;

    // Return sorted NUMA node IDs from the local topology. The topology has one
    // cpu:N entry per node regardless of NIC presence.
    [[nodiscard]] std::vector<int> GetNumaNodes() const;

    tl::expected<Replica::Descriptor, ErrorCode> GetPreferredReplica(
        const std::vector<Replica::Descriptor>& replica_list);

    std::unordered_set<std::string> GetLocalEndpoints() const {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        std::unordered_set<std::string> endpoints;
        for (const auto& [segment_id, segment] : mounted_segments_) {
            endpoints.insert(segment.te_endpoint);
        }
        return endpoints;
    }

    bool CanUseLocalMemcpy(const Replica::Descriptor& replica) const {
        if (!replica.is_memory_replica()) return false;
        return CanUseLocalMemcpy(replica.get_memory_descriptor()
                                     .buffer_descriptor.transport_endpoint_);
    }

    /**
     * @brief Whether a DFS replica is served by an fs adapter that accepts
     * arbitrary host read buffers, so a host destination can be read into
     * directly without a staging buffer.
     */
    bool CanReadDfsReplicaIntoHostMemory(
        const Replica::Descriptor& replica) const;

    /**
     * @brief Check if local hot cache is enabled
     * @return true if hot cache is enabled, false otherwise
     */
    bool IsHotCacheEnabled() const { return hot_cache_ != nullptr; }

    /**
     * @brief Get the local hot cache instance.
     * @return shared_ptr to LocalHotCache, or nullptr if disabled.
     */
    std::shared_ptr<LocalHotCache> GetHotCache() const { return hot_cache_; }

    /**
     * @brief Get the number of cache blocks in local hot cache
     * @return Number of cache blocks if hot cache is enabled, 0 otherwise
     */
    size_t GetLocalHotCacheBlockCount() const {
        if (hot_cache_ != nullptr) {
            return hot_cache_->GetCacheSize();
        }
        return 0;
    }

    bool is_ping_healthy() const { return last_ping_success_.load(); }

    /**
     * @brief Get current frequency admission count for a key.
     * @return estimated count, or 0 if admission sketch is disabled.
     */
    uint8_t GetAdmissionCount(const std::string& key) const {
        if (admission_sketch_ == nullptr) {
            return 0;
        }
        return admission_sketch_->count(key);
    }

    /**
     * @brief Decide whether a key should be admitted to local hot cache.
     * Updates admission sketch only when cache was not used.
     */
    bool ShouldAdmitToHotCache(const std::string& key, bool cache_used) {
        if (!(hot_cache_ && !cache_used)) {
            return false;
        }
        if (admission_sketch_ == nullptr) {
            return true;
        }
        return admission_sketch_->increment(key) >= admission_threshold_;
    }

    bool IsReplicaOnLocalMemory(const Replica::Descriptor& replica);

    // First half of BatchPut only (size-only slices → StartBatchPut).
    // Used by RealClient put sessions; not a Master API facade.
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    StartBatchPutForSizes(const std::vector<std::string>& keys,
                          const std::vector<uint64_t>& object_sizes,
                          const ReplicateConfig& config);

    // Finalize/revoke a put session for a batch of keys. Thin wrappers over
    // the master client, exposed so RealClient put sessions can end/revoke
    // without touching Client internals. Same RPC path as FinalizeBatchPut.
    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const std::vector<ObjectMeta>& object_metas,
        ReplicaType replica_type = ReplicaType::ALL);
    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const std::vector<std::string>& keys,
        ReplicaType replica_type = ReplicaType::ALL);

    // What the dangling-replica probe established for one key. The Put path
    // retries only on kEvicted; the batch-get path also retries on kPresent,
    // because a batch read dies wholesale and a healthy key swept in by a
    // sibling's wiped file must be re-read, not just left unevicted.
    enum class DiskReplicaHealResult {
        // Not a lone client-owned LOCAL_DISK replica; nothing to heal.
        kNotDangling,
        // The probe could not answer (transport/config trouble); leave
        // everything as it was.
        kUnknown,
        // The backing file provably exists; nothing was evicted.
        kPresent,
        // The backing file is proven gone and the dangling replica was
        // evicted, so the operation can be retried.
        kEvicted,
    };

    // When PutStart reports OBJECT_ALREADY_EXISTS, check whether every
    // completed replica is a LOCAL_DISK replica owned by this client, and ask
    // the installed probe (FileStorage::Exists over the offload files) whether
    // the backing file is gone (issue #3709). Only then evict the dangling
    // replica so the Put can be retried; any other outcome keeps the old
    // idempotent path. The batch-get path uses the same heal after a failed
    // SSD read (#3884).
    DiskReplicaHealResult healDanglingLocalDiskReplica(const ObjectKey& key);

    // Reads one byte window of each DISK or DFS replica. `windows[i]` receives
    // `windows[i].size` bytes starting at `value_offsets[i]` in the value.
    std::vector<tl::expected<void, ErrorCode>> BatchReadFileReplicaRanges(
        const std::vector<std::string>& keys,
        const std::vector<Replica::Descriptor>& replicas,
        const std::vector<Slice>& windows,
        const std::vector<uint64_t>& value_offsets);

   protected:
    /**
     * @brief Constructor exposed to subclasses for testing only; production
     * code must go through Create().
     */
    Client(const std::string& local_hostname,
           const std::string& metadata_connstring, const std::string& protocol,
           const std::map<std::string, std::string>& labels = {},
           const std::string& tenant_id = "default");

    /**
     * @brief Prepare and use the storage backend for persisting data.
     * Exposed to subclasses for testing only.
     * @return ErrorCode::OK on success. On failure no storage backend is
     * retained, so persistence stays disabled.
     */
    ErrorCode PrepareStorageBackend(const std::string& storage_root_dir,
                                    const std::string& fsdir,
                                    bool enable_eviction = true,
                                    uint64_t quota_bytes = 0);

   private:
    ReadReplicaValidator read_replica_validator_;

    /**
     * @brief Internal helper functions for initialization and data transfer
     */
    ErrorCode ConnectToMaster(const std::string& master_server_entry);
    // The BatchPutStart counterpart: heals the OBJECT_ALREADY_EXISTS subset
    // with one batched replica-list query and one batched evict, then retries
    // PutStart for just the evicted keys and splices the responses back, so a
    // healthy idempotent batch pays one extra RPC and no evictions.
    void healDanglingLocalDiskBatchStarts(
        std::vector<PutOperation>& ops,
        const std::vector<size_t>& active_indices,
        const std::vector<size_t>& already_exists,
        const std::vector<std::string>& keys,
        const std::vector<std::vector<uint64_t>>& slice_lengths,
        const ReplicateConfig& config);

    void EnterHaRuntimeMode();
    ErrorCode InitTransferEngine(
        const std::string& local_hostname,
        const std::string& metadata_connstring, const std::string& protocol,
        const std::optional<std::string>& device_names);
    void InitTransferSubmitter();
    // Submits one transfer without waiting on it; callers retain slices and
    // replicas until the returned future completes.
    tl::expected<TransferFuture, ErrorCode> SubmitSingleTransfer(
        const Replica::Descriptor& replica_descriptor,
        std::vector<Slice>& slices, TransferRequest::OpCode op_code,
        TransferIntent intent = TransferIntent::kUnspecified);
    ErrorCode TransferData(
        const Replica::Descriptor& replica_descriptor,
        std::vector<Slice>& slices, TransferRequest::OpCode op_code,
        TransferIntent intent = TransferIntent::kUnspecified,
        std::set<std::string>* failed_endpoints = nullptr,
        const std::vector<VSegmentLayoutEntry>* layouts = nullptr);
    ErrorCode TransferReadInternal(
        const Replica::Descriptor& replica_descriptor,
        std::vector<Slice>& slices, uint64_t src_offset,
        const std::vector<VSegmentLayoutEntry>* layouts = nullptr);
    // Internal async range submission helpers (return the transfer future).
    std::optional<TransferFuture> SubmitRangeRead(
        const Replica::Descriptor& replica_descriptor,
        std::vector<Slice>& slices, uint64_t src_offset);
    std::optional<TransferFuture> SubmitRangeWrite(
        const Replica::Descriptor& replica_descriptor,
        std::vector<Slice>& slices, uint64_t dst_offset);
    ErrorCode TransferWrite(
        const Replica::Descriptor& replica_descriptor,
        std::vector<Slice>& slices,
        TransferIntent intent = TransferIntent::kUnspecified,
        std::set<std::string>* failed_endpoints = nullptr,
        const std::vector<VSegmentLayoutEntry>* layouts = nullptr);
    // Submits all memory and NoF writes before waiting, preserving overlap.
    std::vector<std::pair<ReplicaType, ErrorCode>> WriteMemoryAndNofReplicas(
        const std::vector<Replica::Descriptor>& replicas,
        std::vector<Slice>& slices,
        std::set<std::string>* failed_endpoints = nullptr,
        const std::vector<VSegmentLayoutEntry>* layouts = nullptr);
    ErrorCode TransferRead(
        const Replica::Descriptor& replica_descriptor,
        std::vector<Slice>& slices,
        TransferIntent intent = TransferIntent::kUnspecified,
        const std::vector<VSegmentLayoutEntry>* layouts = nullptr);
    ErrorCode TransferReadRange(
        const Replica::Descriptor& replica_descriptor,
        std::vector<Slice>& slices, uint64_t src_offset,
        const std::vector<VSegmentLayoutEntry>* layouts = nullptr);
    ErrorCode ReadDfsReplica(const std::string& key,
                             const Replica::Descriptor& replica_descriptor,
                             std::vector<Slice>& slices);
    void ReconcileMissingKvcsReplica(
        const std::string& key, const QueryResult& query_result,
        const Replica::Descriptor& replica_descriptor);

    tl::expected<uint64_t, ErrorCode> ComputeObjectChecksumForSlices(
        const std::string& object_key, const std::vector<Slice>& slices,
        size_t object_size);

    void PutToLocalFile(const std::string& object_key,
                        const std::vector<Slice>& slices,
                        const DiskDescriptor& disk_descriptor);
    /**
     * @brief Initialize local hot cache
     * @return ErrorCode::OK if disabled or initialized successfully;
     * ErrorCode::INVALID_PARAMS if cache allocation or registration fails
     */
    ErrorCode InitLocalHotCache();

    /**
     * @brief Unregister local hot cache backing memory from TransferEngine.
     */
    void UnregisterLocalHotCacheMemory();

    /**
     * @brief Redirect replica descriptor to local hot cache if cache hit
     * @param key Object key
     * @param replica Replica descriptor
     * @return true if cache hit and replica descriptor was updated, false
     * otherwise
     */
    bool RedirectToHotCache(const std::string& key,
                            Replica::Descriptor& replica);

    /**
     * @brief Asynchronously process slices and update hot cache for TE
     * transfers.
     * @param key Object key.
     * @param slices Vector of slices to check and cache.
     * @param replica Replica descriptor to identify slice sources.
     */
    void ProcessSlicesAsync(const std::string& key,
                            const std::vector<Slice>& slices,
                            const Replica::Descriptor& replica);

    /**
     * @brief Find the first complete replica from a replica list
     * @param replica_list List of replicas to search through
     * @param replica the first complete replica (file or memory)
     * @return ErrorCode::OK if found, ErrorCode::INVALID_REPLICA if no complete
     * replica
     */
    ErrorCode FindFirstCompleteReplica(
        const std::vector<Replica::Descriptor>& replica_list,
        Replica::Descriptor& replica) const;

    /**
     * @brief Batch put helper methods for structured approach
     */
    std::vector<PutOperation> CreatePutOperations(
        const std::vector<ObjectKey>& keys,
        const std::vector<std::vector<Slice>>& batched_slices);
    void StartBatchPut(std::vector<PutOperation>& ops,
                       const ReplicateConfig& config);
    void ComputeBatchObjectChecksums(std::vector<PutOperation>& ops);
    void StageWriteBuffersForRemoteReplicas(std::vector<PutOperation>& ops,
                                            const WriteBufferStager& stager);
    void SubmitTransfers(std::vector<PutOperation>& ops, bool group_by_target);
    void WaitForTransfers(std::vector<PutOperation>& ops);
    void SubmitDfsWrites(std::vector<PutOperation>& ops);
    void FinalizeBatchPut(std::vector<PutOperation>& ops);
    void StartBatchUpsert(std::vector<PutOperation>& ops,
                          const ReplicateConfig& config);
    void FinalizeBatchUpsert(std::vector<PutOperation>& ops);
    std::vector<tl::expected<void, ErrorCode>> CollectResults(
        const std::vector<PutOperation>& ops);
    // 上一批次 Put 是否含失败 key（relaxed atomic：观测性辅助，非正确性；
    // 并发批量下尽力转变检测）。失败→恢复的转变批次打一条 INFO——与
    // CollectResults 的逐 key ERROR 对应，作为「客户端写入恢复」的明确
    // 观测点（如 SLOT_MIGRATING 风暴后 master 就绪、写入重新流通），
    // 稳态成功批次静默。
    std::atomic<bool> last_batch_put_failed_{false};

    std::vector<ErrorCode> WriteDfsReplicas(
        const std::vector<std::string>& keys,
        const std::vector<const std::vector<Slice>*>& slice_lists,
        const std::vector<DistributedFSDescriptor>& descriptors);

    ReplicateConfig AttachHostId(const ReplicateConfig& config) const;
    HcaAffinity MakeHcaAffinity() const;
    void AttachHcaAffinity(ReplicateConfig& config,
                           const std::vector<Slice>& slices) const;
    ReplicateConfig AttachBatchHcaAffinity(
        const ReplicateConfig& config, const std::vector<PutOperation>& ops,
        const std::vector<size_t>& indices) const;

    /**
     * @brief 方案二被动层：向 master 上报传输失败副本的坏端点。
     * 多 submaster 下广播；上报失败仅告警，不阻塞主流程。
     * 需在延迟指标采集之后调用，避免 RPC 耗时污染延迟统计。
     */
    void ReportFailedEndpointsToMasters(
        const std::set<std::string>& failed_endpoints);

    // Client identification
    const UUID client_id_;

    // Client-side metrics
    std::unique_ptr<ClientMetric> metrics_;

    // Core components
    std::shared_ptr<TransferEngine> transfer_engine_;
    MasterClient master_client_;
    std::unique_ptr<TransferSubmitter> transfer_submitter_;
    // NoF initiator injected by RealClient; nullptr when NoF is unavailable.
    std::shared_ptr<NVMeoFInitiator> nof_initiator_;
    std::shared_ptr<vsegment::VSegmentTransferPlanner>
        vsegment_transfer_planner_;
    // Registered client memory used when Copy/Move reads a non-local source
    // (notably a striped vsegment) before writing ordinary target replicas.
    std::shared_ptr<ClientBufferAllocator> replica_transfer_staging_allocator_;

    // Mutex to protect mounted_segments_
    mutable std::mutex mounted_segments_mutex_;
    std::unordered_map<UUID, Segment, boost::hash<UUID>> mounted_segments_;

    // Segments in graceful unmount: readable by remote peers, not allocatable
    // locally. TE MR remains registered until master confirms removal.
    std::unordered_map<UUID, Segment, boost::hash<UUID>>
        gracefully_unmounting_segments_;
    std::unordered_map<UUID, std::function<void(const UUID&)>,
                       boost::hash<UUID>>
        graceful_unmount_cleanup_callbacks_;

    /**
     * @brief Internal helper to unmount a segment by iterator.
     *        Caller must hold mounted_segments_mutex_.
     */
    tl::expected<void, ErrorCode> UnmountSegmentImpl(
        std::unordered_map<UUID, Segment, boost::hash<UUID>>::iterator it);

    void StartGracefulUnmountTimer(const UUID& segment_id,
                                   uint64_t grace_period_ms);
    void OnGracefulUnmountTimer(const UUID& segment_id, int retry_left);
    bool WaitForGracefulUnmountDelay(std::chrono::milliseconds delay);
    std::mutex graceful_unmount_timer_mutex_;
    std::condition_variable graceful_unmount_timer_cv_;
    bool graceful_unmount_timer_stopping_{false};

    // Configuration
    const std::string local_hostname_;
    const std::string host_id_;
    const std::string metadata_connstring_;
    const std::string protocol_;
    const bool object_checksum_enabled_;

    // Client persistent thread pool for async operations
    // Pinned host memory pool for GPU D2H staging (must outlive
    // write_thread_pool_)
    std::unique_ptr<PinnedBufferPool> pinned_buffer_pool_;
    ThreadPool write_thread_pool_;
    std::shared_ptr<StorageBackend> storage_backend_;
    std::shared_ptr<DistributedStorageBackend> dfs_storage_backend_;

    // Probe used by healDanglingLocalDiskReplica to prove a completed
    // LOCAL_DISK replica's backing file is gone. Installed by the owner of
    // the offload file storage (RealClient); unset means no local-disk
    // offload is active and the heal stays inert. true: file is gone,
    // false: file present, nullopt: unknown (keep the old path).
    std::function<std::optional<bool>(const std::string& key)>
        local_disk_probe_fn_;

    // For high availability
    std::unique_ptr<ha::LeaderCoordinator> leader_coordinator_;
    std::mutex leader_switch_mutex_;
    std::optional<ha::MasterView> current_master_view_;
    std::string direct_master_address_;
    // Etcd endpoints parsed from the HA backend entry, used to load the KV
    // partition routing snapshot. Empty in non-HA mode.
    std::string etcd_connstring_;
    // Resolved cluster namespace (cluster_id) cached after the first routing
    // load; reused by the periodic routing refresh loop.
    std::string cluster_id_;
    // CVM multi-submaster mode: set when the client bootstraps from the
    // /cvm/<ns>/masters/ registry (no single-leader master_view in etcd).
    // Empty in single-leader HA mode and direct mode. Also drives the
    // ping-failure re-discovery path.
    std::string cvm_cluster_namespace_;
    // CVM 多 submaster：已发现的全部 primary submaster 地址（含当前连接的
    // 那个）。用于 client 侧全量 mount（同一 segment 挂到所有 submaster）与
    // 多 submaster 心跳（防止各 submaster 因收不到 ping 而误卸载 segment）。
    // 由 ConnectToCvmSubmasters 填充。
    std::mutex cvm_submaster_addresses_mutex_;
    std::vector<std::string> cvm_submaster_addresses_;
    // LOCAL_DISK 段对称生命周期状态（RefreshSubmasterAddresses 增量接线
    // 用）：mount 成功后置位；新 submaster 上线时据此补挂 LOCAL_DISK 段
    // 并重报容量，移除时定向卸载，保证各 master 容量记账对称闭合。
    std::atomic<bool> local_disk_mounted_{false};
    std::atomic<bool> local_disk_offload_enabled_{false};
    // 最近一次成功上报的 SSD 总容量（<=0 表示未上报过），补挂时重报。
    std::atomic<int64_t> ssd_total_capacity_bytes_{0};
    // 串行化 TryLoadRoutingOnce（及其内部对 cluster_id_ 的读写），
    // 用于 routing-refresh 线程与 SLOT_NOT_OWNED 触发的按需刷新之间。
    std::mutex routing_load_mutex_;
    std::thread leader_monitor_thread_;
    std::atomic<bool> leader_monitor_running_{false};
    std::thread routing_refresh_thread_;
    std::atomic<bool> routing_refresh_running_{false};
    std::thread storage_heartbeat_thread_;
    std::atomic<bool> storage_heartbeat_running_{false};
    std::thread task_poll_thread_;
    std::atomic<bool> task_poll_running_{false};
    std::atomic<bool> last_ping_success_{false};
    std::atomic<bool> segment_desc_publish_pending_{false};
    std::atomic<bool> rpc_meta_publish_pending_{false};
    ErrorCode ConnectMasterEndpoint(const std::string& address);
    ErrorCode SwitchLeader(const ha::MasterView& target_view);
    // CVM multi-submaster bootstrap/failover: connect the etcd store client,
    // discover the cluster namespace from /cvm/<ns>/masters/, and connect to
    // the first live primary submaster (first-registered first, matching the
    // CvmController ranking). On success sets cvm_cluster_namespace_.
    ErrorCode ConnectToCvmSubmasters(const std::string& etcd_endpoints);
    void LoadPartitionRouting();
    void TryLoadRoutingOnce();
    void RoutingRefreshThreadMain();
    void LeaderMonitorThreadMain();
    void StorageHeartbeatThreadMain();
    // CVM 多 submaster：向所有 primary submaster 定向 Ping（不切换当前连接），
    // 各 submaster 收到 ping 后会保活该 client；返回 NEED_REMOUNT 的 submaster
    // 会被重新 mount（重新注册 segment）。单 submaster 失败不影响其他。
    void HeartbeatAllSubmasters();
    // CVM 多 submaster：重新扫描 /cvm/<ns>/masters/ 注册表，更新已发现的
    // primary 地址列表；对新增的 submaster 全量 mount 所有本地 segment，使
    // 新加入的 submaster 也具备给本 client 分配副本的能力。由路由刷新线程
    // 周期调用。
    void RefreshSubmasterAddresses();
    void TaskPollThreadMain();
    void EnsureStorageControlPlaneStarted();
    void PollAndDispatchTasks();
    void SubmitTask(const TaskAssignment& assignment);

    // For task management
    // Client-side task representation
    struct ClientTask {
        TaskAssignment assignment;
        uint32_t retry_count = 0;

        void increment_retry() { retry_count++; }
    };

    void ExecuteTask(const ClientTask& client_task);

    tl::expected<void, ErrorCode> ExecuteReplicaTransfer(
        const std::string& key, const std::string& action_name,
        std::function<tl::expected<void, ErrorCode>()> end_fn,
        std::function<tl::expected<void, ErrorCode>()> revoke_fn,
        const Replica::Descriptor& source,
        const std::vector<Replica::Descriptor>& targets,
        const std::vector<VSegmentLayoutEntry>* layouts);

    /**
     * @brief Copy an object's replica to target segments
     * @param key Object key
     * @param source Source segment
     * @param targets Target segments
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    tl::expected<void, ErrorCode> Copy(const std::string& key,
                                       const std::string& source,
                                       const std::vector<std::string>& targets);
    tl::expected<void, ErrorCode> Copy(const std::string& key,
                                       const std::string& tenant_id,
                                       const std::string& source,
                                       const std::vector<std::string>& targets);
    tl::expected<void, ErrorCode> Copy(
        const std::string& key, const std::string& tenant_id,
        const std::string& source, const std::vector<std::string>& targets,
        const UUID& dynamic_replication_lease_id,
        uint64_t dynamic_replication_version_epoch);

    /**
     * @brief Move an object's replica from source segment to target segment
     * @param key Object key
     * @param source Source segment
     * @param target Target segment
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    tl::expected<void, ErrorCode> Move(const std::string& key,
                                       const std::string& source,
                                       const std::string& target);
    tl::expected<void, ErrorCode> Move(const std::string& key,
                                       const std::string& tenant_id,
                                       const std::string& source,
                                       const std::string& target);

    // Task thread pool for async task execution
    ThreadPool task_thread_pool_;
    std::atomic<bool> task_running_{true};

    // Task polling configuration
    static constexpr size_t kTaskBatchSize =
        16;  // Number of tasks to fetch per poll

    bool te_initialized_{false};

    // Local hot cache and async handler
    std::shared_ptr<LocalHotCache> hot_cache_;
    std::unique_ptr<LocalHotCacheHandler> hot_cache_handler_;
    bool hot_cache_memory_registered_{false};

    // Frequency admission: only cache keys whose CMS count >= threshold
    std::unique_ptr<CountMinSketch> admission_sketch_;
    uint8_t admission_threshold_ = 2;
};

}  // namespace mooncake
