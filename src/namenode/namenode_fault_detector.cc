#include "namenode_fault_detector.h"

#include <iostream>

namespace mini_storage
{
    FaultDetector::FaultDetector(MetadataStore *metadata, DataNodeManager *dn_manager, int interval_sec)
        : metadata_(metadata), dn_manager_(dn_manager), interval_sec_(interval_sec)
    {
        rep_monitor_ = std::make_unique<ReplicationMonitor>(metadata_, dn_manager_);
        replicator_ = std::make_unique<BlockReplicator>(metadata_, dn_manager_);
        checker_ = std::make_unique<ConsistencyChecker>(metadata_, dn_manager_);
    }

    FaultDetector::~FaultDetector()
    {
        Stop();
    }

    void FaultDetector::Start()
    {
        stop_.store(false);
        detect_thread_ = std::thread([this] { DetectLoop(); });
        std::cout << "[FaultDetector] Started (interval=" << interval_sec_ << "s)\n";
    }

    void FaultDetector::Stop()
    {
        stop_.store(true);
        if (detect_thread_.joinable()) detect_thread_.join();
    }

    // 立即执行一次完整的故障检测+修复（供测试直接调用）
    void FaultDetector::RunOnce()
    {
        std::cout << "\n[FaultDetector] === Round " << repair_rounds_.load() + 1
                << " ===\n";

        // Step 1: 检测宕机节点
        HandleDeadNodes();

        // Step 2: 修复副本
        RepairReplicas();

        // Step 3: 一致性校验（轻量，只报告问题，不修复数据内容）
        VerifyConsistency();

        repair_rounds_.fetch_add(1);
    }

    // 按固定周期反复执行一次完整的故障检测与修复
    void FaultDetector::DetectLoop()
    {
        // 只有调用Stop后台线程才会退出
        while(!stop_.load())
        {
            // 分散等待，每 100ms 检查一次 stop_ 标志
            for (int i = 0; i < interval_sec_ * 10 && !stop_.load(); ++i)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            if (stop_.load()) break;
            RunOnce();
        }
    }

    void FaultDetector::HandleDeadNodes()
    {
        // CheckDataNodeHealth 更新 DN 状态（ALIVE/SUSPECT/DEAD）
        dn_manager_->CheckDataNodeHealth();

        // 打印当前集群状态
        auto health = rep_monitor_->GetClusterHealth();
        std::cout << "[FaultDetector] Cluster: "
                  << health.alive_datanodes << " alive DN, "
                  << health.dead_datanodes << " dead DN | "
                  << health.total_blocks << " blocks total | "
                  << health.healthy_blocks << " healthy, "
                  << health.under_replicated << " under-replicated, "
                  << health.lost_blocks << " lost\n";
    }

    void FaultDetector::RepairReplicas()
    {
        auto under_replicated = rep_monitor_->ScanUnderReplicated();
        if (under_replicated.empty())
        {
            std::cout << "[FaultDetector] All blocks have sufficient replicas ✓\n";
            return;
        }

        std::cout << "[FaultDetector] Found " << under_replicated.size()
                << " under-replicated block(s), starting repair...\n";

        int repaired = replicator_->RepairUnderReplicated(under_replicated);
        blocks_repaired_.fetch_add(repaired);

        std::cout << "[FaultDetector] Repaired " << repaired << "/"
                << under_replicated.size() << " blocks\n";
    }

    void FaultDetector::VerifyConsistency()
    {
        auto bad = checker_->ScanAllBlocks();
        if (!bad.empty())
        {
            std::cout << "[FaultDetector] Consistency issues found:\n";
            checker_->PrintReport(bad);
        }
        else
        {
            std::cout << "[FaultDetector] Consistency check OK ✓\n";
        }

        return;
    }

} // namespace mini_storage