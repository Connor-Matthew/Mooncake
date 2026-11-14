#!/usr/bin/env python3
# Tool to remotely create SPDK targets on multiple nodes
# Usage: python3 -m mooncake.spdk_tgt_create --spdk_target_info="ip:192.168.65.56 path:/home/spdk pci:0000:01:00.0,0000:02:00.0" --spdk_target_info="ip:192.168.65.57 path:/home/spdk"
#
# Modified By Yida (v6): URMA 大 I/O 部署（ADR-0017，对照 SPDK fork 的
# target_nvme_takeover.sh）。URMA 数据路径要求单 buffer（iovcnt==1），
# --max-io-size 超过默认 large_bufsize（135168）时必须重配 iobuf 池：
#   python3 -m mooncake.spdk_tgt_create --transport-type URMA \
#     --spdk_target_info="ip:10.0.0.2 path:/home/x/spdk" \
#     --urma-env SPDK_URMA_DEV_NAME=udmac0d1e2 \
#     --max-io-size 2097152 --iobuf-options "16384,8192,320,2097152"

from __future__ import annotations

import argparse
import logging
import os
import re
import shlex
import time
from typing import TYPE_CHECKING, List, Dict, Any, Optional

from mooncake._administration import require_paramiko

if TYPE_CHECKING:
    import paramiko


def _core_mask_popcount(core_mask: str) -> int:
    """Modified By Yida (v6): core mask（0x 十六进制或十进制）的置位数。

    解析失败（如 '0-3' 核列表形式）返回 0，调用方据此跳过槽位告警。
    """
    try:
        value = int(str(core_mask), 0)
        return bin(value).count('1') if value > 0 else 0
    except ValueError:
        return 0


class SPDKTgtCreator:
    """
    Remotely creates SPDK targets on multiple nodes via SSH.
    """

    # Modified By Yida (v3): 修正 max_io_size / in_capsule_data_size 的默认值
    # 与 RPC flag 映射。SPDK CLI 中 -c = in_capsule_data_size、-i = max_io_size；
    # 旧代码 flag 与默认值同时反置（数值上恰好抵消），只改一处都会让实际行为
    # 变化，因此两处必须一起改：max_io_size=131072(-i)、
    # in_capsule_data_size=4096(-c)。
    DEFAULT_TRANSPORT_OPTIONS = {
        'trtype': 'RDMA',
        'max_queue_depth': 128,
        'max_io_qpairs_per_ctrlr': 127,
        'max_io_size': 131072,
        'in_capsule_data_size': 4096,
        'io_unit_size': 131072,
        'max_aq_depth': 128,
        'num_shared_buffers': 4096,
        'buf_cache_size': 32,
    }

    TRANSPORT_RPC_FLAGS = {
        'trtype': '-t',
        'max_queue_depth': '-q',
        'max_io_qpairs_per_ctrlr': '-m',
        'max_io_size': '-i',
        'in_capsule_data_size': '-c',
        'io_unit_size': '-u',
        'max_aq_depth': '-a',
        'num_shared_buffers': '-n',
        'buf_cache_size': '-b',
    }

    # Modified By Yida (v3): nvmf_tgt 进程必须携带的 URMA 环境变量。
    # 注意这些变量必须出现在 nvmf_tgt 进程环境中——只在调用 rpc.py 的
    # shell 中设置没有作用。包含 JETTY_COUNT（SPDK nvme/nvmf 侧都会读取，
    # 旧列表遗漏）。MC_URMA_* 为兼容别名，同样转发。
    URMA_ENV_VARS = (
        'SPDK_URMA_DEV_NAME',
        'SPDK_URMA_EID_INDEX',
        'SPDK_URMA_TRANS_MODE',
        'SPDK_URMA_ACTIVE_PORT',
        'SPDK_URMA_BONDING_BALANCE',
        'SPDK_URMA_BONDING_MULTIPATH_ENABLE',
        'SPDK_URMA_JFC_COUNT',
        'SPDK_URMA_JFC_DEPTH',
        'SPDK_URMA_JETTY_COUNT',
        'SPDK_URMA_JETTY_DEPTH',
        'SPDK_URMA_MAX_IO_SIZE',
    )

    # Modified By Yida (v6): ADR-0017 大 I/O 部署经验。URMA 数据路径要求单
    # buffer（iovcnt==1），max_io_size 超过默认 large_bufsize 后必须重配
    # iobuf 池：nvmf_tgt 以 --wait-for-rpc 启动，startup 窗口内
    # iobuf_set_options 配好池子再 framework_start_init（iobuf_set_options
    # 仅 SPDK_RPC_STARTUP 可调）。
    DEFAULT_LARGE_BUFSIZE = 135168  # lib/thread/iobuf.c 默认 large bufsize
    IOBUF_MIN = {
        'small_count': 64,      # IOBUF_MIN_SMALL_POOL_SIZE
        'large_count': 8,       # IOBUF_MIN_LARGE_POOL_SIZE
        'small_bufsize': 4096,  # IOBUF_MIN_SMALL_BUFSIZE
        'large_bufsize': 8192,  # IOBUF_MIN_LARGE_BUFSIZE
    }
    # 自定义 iobuf 池时 nvmf_create_transport 的每 PG 缓存压制值。默认"自动
    # 吃大池一半再按已有 PG 数均分"（transport.c:640），首个 PG 独吞 pool/2，
    # 叠加每核 bdev(16)+accel(16) 个 large 预占后，add_ns 建通道时池子必被
    # 抽干（populate 0/16，ADR-0017 问题 1）。large 缓存下限：必须 >= 每 PG
    # 峰值并发，否则 target 注册缓存（128 个、只进不出）命中率崩塌。
    IOBUF_LARGE_CACHE_DEFAULT = 32
    IOBUF_SMALL_CACHE_DEFAULT = 1024

    def __init__(self, spdk_targets: List[str], transport_options: Dict[str, Any] = None, core_mask: str = '0xff', port: int = 22, urma_env: Dict[str, str] = None, iobuf_options: Optional[str] = None, iobuf_large_cache_size: Optional[int] = None):
        self.spdk_targets = spdk_targets
        self.core_mask = core_mask
        self.port = port
        self.transport_options = dict(self.DEFAULT_TRANSPORT_OPTIONS)
        if transport_options:
            self.transport_options.update(transport_options)
        self.urma_env = self._collect_urma_env(urma_env)
        # Modified By Yida (v6): iobuf 池四元组（小池数量,小池buf,大池数量,大池buf）
        self.iobuf = self._parse_iobuf_options(iobuf_options)
        self.iobuf_large_cache_size = iobuf_large_cache_size
        if self.iobuf and self.iobuf_large_cache_size is None:
            self.iobuf_large_cache_size = self.IOBUF_LARGE_CACHE_DEFAULT
        self._setup_logging()
        self.target_configs = self._parse_spdk_targets()
        self._validate_large_io_config()

    def _collect_urma_env(self, explicit: Optional[Dict[str, str]]) -> Dict[str, str]:
        """收集要注入 nvmf_tgt 进程的 URMA 环境变量。

        来源优先级（后者覆盖前者）：
        1. 调用 shell 中已设置的 SPDK_URMA_* / MC_URMA_* 变量（自动转发）
        2. --urma-env NAME=VALUE 显式指定
        """
        env: Dict[str, str] = {}
        for name, value in os.environ.items():
            if name.startswith('SPDK_URMA_') or name.startswith('MC_URMA_'):
                env[name] = value
        if explicit:
            env.update(explicit)
        return env

    def _parse_iobuf_options(self, spec: Optional[str]) -> Optional[Dict[str, int]]:
        """Modified By Yida (v6): 解析 iobuf 池四元组并校验 iobuf.c 下限。"""
        if not spec:
            return None
        parts = [p.strip() for p in spec.split(',')]
        if len(parts) != 4:
            raise ValueError(
                f"Invalid --iobuf-options: {spec!r} (expected "
                "'small_count,small_bufsize,large_count,large_bufsize')")
        try:
            keys = ('small_count', 'small_bufsize', 'large_count', 'large_bufsize')
            iobuf = {k: int(v) for k, v in zip(keys, parts)}
        except ValueError:
            raise ValueError(
                f"Invalid --iobuf-options: {spec!r} (values must be integers)")
        for key, minimum in self.IOBUF_MIN.items():
            if iobuf[key] < minimum:
                raise ValueError(
                    f"iobuf {key}={iobuf[key]} below minimum {minimum} "
                    "(lib/thread/iobuf.c)")
        return iobuf

    def _validate_large_io_config(self) -> None:
        """Modified By Yida (v6): 大 I/O 与 iobuf 池的配置校验（ADR-0017）。"""
        if self.iobuf is None:
            if (str(self.transport_options.get('trtype', '')).upper() == 'URMA'
                    and self.transport_options.get('max_io_size', 0) > self.DEFAULT_LARGE_BUFSIZE):
                raise ValueError(
                    f"URMA max_io_size={self.transport_options['max_io_size']} exceeds "
                    f"the default large_bufsize ({self.DEFAULT_LARGE_BUFSIZE}); URMA "
                    "requires a single data buffer (iovcnt==1). Pass --iobuf-options, "
                    f"e.g. \"16384,8192,320,{self.transport_options['max_io_size']}\"")
            return
        if self.iobuf['large_bufsize'] < self.transport_options.get('max_io_size', 0):
            raise ValueError(
                f"iobuf large_bufsize={self.iobuf['large_bufsize']} < max_io_size="
                f"{self.transport_options['max_io_size']}: URMA data path requires a "
                "single buffer (iovcnt==1); raise the 4th field of --iobuf-options")

        # 槽位账按"每核固定预占"算，不是字节账（ADR-0017 问题 5）
        ncore = _core_mask_popcount(self.core_mask)
        if ncore <= 0:
            return
        large_eager = ncore * (32 + (self.iobuf_large_cache_size
                                     or self.IOBUF_LARGE_CACHE_DEFAULT))
        if self.iobuf['large_count'] <= large_eager:
            self.logger.warning(
                "large pool %d cannot cover per-core fixed reservation: cores=%d x "
                "(32 bdev/accel + %d PG cache) = %d; startup/add_ns will fail with "
                "'populate 0/16'. Lower --iobuf-large-cache-size or raise the 3rd "
                "field of --iobuf-options",
                self.iobuf['large_count'], ncore, self.iobuf_large_cache_size,
                large_eager)
        small_eager = ncore * 1280
        if self.iobuf['small_count'] <= small_eager:
            self.logger.warning(
                "small pool %d cannot cover per-core fixed reservation: cores=%d x "
                "1280 (bdev 128 + accel 128 + PG 1024); raise the 1st field of "
                "--iobuf-options (e.g. 16384)",
                self.iobuf['small_count'], ncore)

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def _parse_spdk_targets(self) -> List[Dict[str, Any]]:
        """
        Parse SPDK target information from command line arguments.
        Format: "ip:<ip> path:<spdk_path> [pci:<pci1>,<pci2> ...]"
        """
        target_configs = []

        for target_info in self.spdk_targets:
            target = {"ip": None, "path": None, "pci_devices": []}

            # Split the target info by spaces
            parts = target_info.split()

            # Simple state machine to parse the target info
            state = None  # Can be 'ip', 'path', or 'pci'

            for part in parts:
                if ":" in part:
                    # This is a key-value pair
                    key, value = part.split(":", 1)
                    key = key.strip()
                    value = value.strip()

                    if key == "ip":
                        target["ip"] = value
                        state = "ip"
                    elif key == "path":
                        target["path"] = value
                        state = "path"
                    elif key == "pci":
                        # Parse PCI devices separated by commas
                        if value:
                            # Split by commas and strip whitespace
                            pci_list = [
                                dev.strip() for dev in value.split(",") if dev.strip()
                            ]
                            target["pci_devices"].extend(pci_list)
                        state = "pci"
                    elif state == "pci":
                        pci_list = [
                            dev.strip() for dev in part.split(",") if dev.strip()
                        ]
                        target["pci_devices"].extend(pci_list)
                else:
                    # This is a continuation of the current state
                    if state == "path":
                        # Path might contain spaces (unlikely but possible)
                        target["path"] += " " + part
                    elif state == "pci":
                        pci_list = [
                            dev.strip() for dev in part.split(",") if dev.strip()
                        ]
                        target["pci_devices"].extend(pci_list)

            # Validate required fields
            if not target["ip"]:
                raise ValueError("Each spdk_target_info must contain 'ip' field")
            if not target["path"]:
                raise ValueError("Each spdk_target_info must contain 'path' field")

            target_configs.append(target)
            pci_info = (
                target["pci_devices"] if target["pci_devices"] else "auto-discover"
            )
            self.logger.info(
                f"Parsed target: IP={target['ip']}, Path={target['path']}, PCI devices={pci_info}"
            )

        return target_configs

    def _ssh_connect(
        self,
        ip: str,
        username: str = "root",
        password: str = None,
        key_file: str = None,
        port: int = 22,
    ) -> paramiko.SSHClient:
        """
        Establish an SSH connection to the target host.
        """
        paramiko = require_paramiko()
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            if key_file:
                self.logger.info(f"Connecting to {ip} using key file {key_file}")
                ssh.connect(ip, port=port, username=username, key_filename=key_file)
            else:
                self.logger.info(f"Connecting to {ip} using password authentication")
                ssh.connect(ip, port=port, username=username, password=password)
            return ssh
        except Exception as e:
            self.logger.error(f"Failed to connect to {ip}: {e}")
            raise

    def _execute_command(
        self,
        ssh: paramiko.SSHClient,
        command: str,
        working_dir: str = None,
        sudo: bool = False,
        log_errors: bool = True,
        timeout: Optional[int] = 30,
    ) -> tuple:
        """
        Execute a command on the remote host via SSH.
        """
        if working_dir:
            command = f"cd {working_dir} && {command}"

        if sudo:
            command = f"sudo {command}"

        self.logger.debug(f"Executing command: {command}")

        stdin, stdout, stderr = ssh.exec_command(command, timeout=timeout)
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode("utf-8")
        error = stderr.read().decode("utf-8")

        if output:
            self.logger.debug(f"Command output: {output}")
        if error:
            self.logger.debug(f"Command error: {error}")
        if exit_status != 0:
            if log_errors:
                self.logger.error(
                    f"Command failed with exit code {exit_status}: {command}"
                )
                if output:
                    self.logger.error(f"Command output: {output}")
                self.logger.error(f"Error output: {error}")
            raise RuntimeError(f"Command execution failed: {error or output}")

        return output, error

    def _discover_nvme_pci_devices(self, ssh: paramiko.SSHClient) -> List[str]:
        """
        Discover SPDK-ready or unmounted NVMe controller PCI addresses on the target host.
        """
        self.logger.info(
            "No PCI devices specified, discovering SPDK-ready or unmounted NVMe PCI devices"
        )
        output, _ = self._execute_command(
            ssh,
            r"""for dev in /sys/bus/pci/devices/*; do
  class=$(cat "$dev/class" 2>/dev/null || true)
  case "$class" in
    0x0108*) ;;
    *) continue ;;
  esac
  pci=$(basename "$dev")
  driver=$(basename "$(readlink "$dev/driver" 2>/dev/null)" 2>/dev/null || true)
  case "$driver" in
    vfio-pci|uio_pci_generic|igb_uio)
      echo "USE $pci $driver"
      continue
      ;;
  esac
  has_block=0
  mounted=0
  for block in /sys/block/nvme*n*; do
    [ -e "$block" ] || continue
    real_device=$(readlink -f "$block/device" 2>/dev/null || true)
    case "$real_device" in
      *"/$pci"/*|*"/$pci") ;;
      *) continue ;;
    esac
    has_block=1
    disk=$(basename "$block")
    if lsblk -nr -o MOUNTPOINT "/dev/$disk" 2>/dev/null | grep -q '[^[:space:]]'; then
        mounted=1
    fi
  done
  if [ "$has_block" -eq 0 ]; then
    echo "SKIP $pci no_block_device"
  elif [ "$mounted" -eq 0 ]; then
    echo "USE $pci"
  else
    echo "SKIP $pci mounted"
  fi
done""",
        )

        pci_devices = []
        pci_pattern = re.compile(
            r"^(?:[0-9a-fA-F]{4}:)?[0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-7]$"
        )
        for line in output.splitlines():
            fields = line.split()
            if len(fields) < 2:
                continue
            action, pci = fields[0], fields[1]
            if action == "USE" and pci_pattern.match(pci):
                pci_devices.append(pci)
            elif action == "SKIP" and pci_pattern.match(pci):
                reason = " ".join(fields[2:]) or "not eligible"
                self.logger.warning(f"Skipping NVMe PCI device {pci}: {reason}")

        if not pci_devices:
            raise RuntimeError(
                "No SPDK-ready or unmounted NVMe PCI devices found on the target host"
            )

        self.logger.info(
            f"Selected NVMe PCI devices for SPDK setup: {', '.join(pci_devices)}"
        )
        return pci_devices

    def _filter_spdk_ready_pci_devices(
        self, ssh: paramiko.SSHClient, pci_devices: List[str], strict: bool
    ) -> List[str]:
        """
        Keep PCI devices that are bound to an SPDK-compatible userspace driver.
        """
        if not pci_devices:
            return []

        pci_args = " ".join(shlex.quote(pci) for pci in pci_devices)
        output, _ = self._execute_command(
            ssh,
            f"""for pci in {pci_args}; do
  driver=$(basename "$(readlink "/sys/bus/pci/devices/$pci/driver" 2>/dev/null)" 2>/dev/null || true)
  case "$driver" in
    vfio-pci|uio_pci_generic|igb_uio) echo "READY $pci $driver" ;;
    "") echo "NOT_READY $pci no_driver" ;;
    *) echo "NOT_READY $pci $driver" ;;
  esac
done""",
        )

        ready_devices = []
        not_ready_devices = []
        for line in output.splitlines():
            fields = line.split()
            if len(fields) < 3:
                continue
            state, pci, driver = fields[0], fields[1], fields[2]
            if state == "READY":
                ready_devices.append(pci)
            elif state == "NOT_READY":
                not_ready_devices.append((pci, driver))

        if not_ready_devices:
            details = ", ".join(
                f"{pci} ({driver})" for pci, driver in not_ready_devices
            )
            if strict:
                raise RuntimeError(
                    "Some PCI devices are not available to SPDK after setup.sh: "
                    f"{details}. Check whether they are mounted or still bound to the kernel driver."
                )
            self.logger.warning(
                f"Skipping PCI devices not available to SPDK after setup.sh: {details}"
            )

        if not ready_devices:
            raise RuntimeError("No PCI devices are available to SPDK after setup.sh")

        self.logger.info(f"PCI devices available to SPDK: {', '.join(ready_devices)}")
        return ready_devices

    def _start_spdk_tgt(self, ssh: paramiko.SSHClient, spdk_path: str, wait_for_rpc: bool = False) -> None:
        """
        Start the SPDK NVMF target service in the background.
        """
        # Check if tgt is already running
        try:
            self._execute_command(ssh, "pgrep -x nvmf_tgt", log_errors=False)
            self.logger.info("SPDK tgt service is already running, stopping it first")
            self._execute_command(ssh, "pkill -x nvmf_tgt")
            time.sleep(2)  # Give it time to stop
        except RuntimeError:
            self.logger.debug("SPDK tgt service is not running, will start it")

        # Start tgt in the background using absolute path
        self.logger.info(f"Starting SPDK tgt service with core mask {self.core_mask}")
        tgt_binary = f"{spdk_path}/build/bin/nvmf_tgt"
        log_file = f"{spdk_path}/tgt.log"
        # Modified By Yida (v3): URMA 环境变量必须出现在 nvmf_tgt 进程环境中。
        # 通过 `env KEY=VALUE` 前缀注入 nohup 启动的进程；未设置任何 URMA
        # 变量时不改变原有启动方式。
        env_prefix = ''
        if self.urma_env:
            env_kv = ' '.join(
                f'{shlex.quote(name)}={shlex.quote(str(value))}'
                for name, value in sorted(self.urma_env.items()))
            env_prefix = f'env {env_kv} '
            self.logger.info(
                "Passing URMA env to nvmf_tgt: %s",
                ', '.join(sorted(self.urma_env)))
        # Modified By Yida (v6): 配 iobuf 池需要 startup 窗口
        # （iobuf_set_options 仅 SPDK_RPC_STARTUP 可调），以 --wait-for-rpc
        # 启动，RPC 配完池子后由 _configure_iobuf 调 framework_start_init。
        wait_flag = '--wait-for-rpc ' if wait_for_rpc else ''
        self._execute_command(
            ssh,
            f"nohup {env_prefix}{tgt_binary} {wait_flag}-m {shlex.quote(self.core_mask)} > {log_file} 2>&1 &",
            timeout=None
        )
        time.sleep(3)  # Give it time to start

    def _wait_for_rpc_ready(self, ssh: paramiko.SSHClient, spdk_path: str, timeout_sec: int = 30) -> None:
        """Modified By Yida (v6): 轮询 rpc_get_methods 直到 RPC 就绪。

        对照 target_nvme_takeover.sh 的就绪判定；替代原来盲目 sleep(3)
        后直接下发 RPC 的方式（--wait-for-rpc 启动时 RPC 就绪更晚）。
        """
        rpc_script = f"{spdk_path}/scripts/rpc.py"
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            try:
                self._execute_command(ssh, f"{rpc_script} rpc_get_methods",
                                      log_errors=False, timeout=10)
                return
            except RuntimeError:
                time.sleep(0.5)
        raise RuntimeError(f"SPDK RPC not ready within {timeout_sec}s")

    def _configure_iobuf(self, ssh: paramiko.SSHClient, spdk_path: str) -> None:
        """Modified By Yida (v6): startup 窗口内配置 iobuf 池并启动 framework。

        仅在 nvmf_tgt 以 --wait-for-rpc 启动后可用；framework_start_init
        之后其余 RPC（bdev/transport/subsystem）按原顺序下发。
        """
        rpc_script = f"{spdk_path}/scripts/rpc.py"
        ib = self.iobuf
        self.logger.info(
            "Configuring iobuf pools: small %dx%d B, large %dx%d B",
            ib['small_count'], ib['small_bufsize'],
            ib['large_count'], ib['large_bufsize'])
        self._execute_command(
            ssh,
            f"{rpc_script} iobuf_set_options "
            f"--small-pool-count {ib['small_count']} "
            f"--small-bufsize {ib['small_bufsize']} "
            f"--large-pool-count {ib['large_count']} "
            f"--large-bufsize {ib['large_bufsize']}")
        self._execute_command(ssh, f"{rpc_script} framework_start_init")

    def _setup_spdk(self, ssh: paramiko.SSHClient, spdk_path: str, pci_devices: List[str]) -> None:
        """
        Setup SPDK with the specified PCI devices.
        """
        self.logger.info(f"Setting up SPDK with PCI devices: {', '.join(pci_devices)}")
        pci_allowed = " ".join(pci_devices)
        setup_script = f"{spdk_path}/scripts/setup.sh"
        self._execute_command(
            ssh, f"PCI_ALLOWED='{pci_allowed}' {setup_script}", sudo=True
        )

    def _format_transport_options(self) -> str:
        """
        Format transport options for the SPDK nvmf_create_transport RPC.
        """
        formatted_options = []
        for option, flag in self.TRANSPORT_RPC_FLAGS.items():
            value = self.transport_options[option]
            formatted_options.extend([flag, shlex.quote(str(value))])
        return " ".join(formatted_options)

    def _create_transport(self, ssh: paramiko.SSHClient, spdk_path: str) -> None:
        """
        Create NVMe-oF transport for SPDK.
        """
        self.logger.info(f"Creating {self.transport_options['trtype']} transport")
        rpc_script = f"{spdk_path}/scripts/rpc.py"
        transport_options = self._format_transport_options()
        if self.iobuf:
            # Modified By Yida (v6): 自定义 iobuf 池时显式压小每 PG 缓存——
            # 默认自动策略会让首个 PG 独吞大池一半，add_ns 报 populate 0/16
            # （ADR-0017 问题 1）。large 缓存必须 >= 每 PG 峰值并发。
            transport_options += (
                f" --iobuf-large-cache-size {self.iobuf_large_cache_size}"
                f" --iobuf-small-cache-size {self.IOBUF_SMALL_CACHE_DEFAULT}")
        self._execute_command(ssh, f"{rpc_script} nvmf_create_transport {transport_options}")

    def _create_bdevs(
        self, ssh: paramiko.SSHClient, spdk_path: str, pci_devices: List[str]
    ) -> List[str]:
        """
        Create block devices for the specified PCI devices.
        """
        bdevs = []
        rpc_script = f"{spdk_path}/scripts/rpc.py"
        for i, pci in enumerate(pci_devices):
            bdev_name = f"Nvme{i}"
            self.logger.info(f"Creating bdev {bdev_name} for PCI {pci}")
            self._execute_command(
                ssh,
                f"{rpc_script} bdev_nvme_attach_controller -b {bdev_name} -t PCIe -a {pci}",
            )
            bdevs.append(f"{bdev_name}n1")
            self.logger.info(f"Attached PCI {pci} as bdev {bdev_name}n1")
        return bdevs

    def _create_subsystem(self, ssh: paramiko.SSHClient, spdk_path: str) -> str:
        """
        Create an NVMF subsystem.
        """
        subsystem_nqn = "nqn.2016-06.io.spdk:cnode1"
        self.logger.info(f"Creating subsystem {subsystem_nqn}")
        rpc_script = f"{spdk_path}/scripts/rpc.py"
        self._execute_command(
            ssh,
            f"{rpc_script} nvmf_create_subsystem {subsystem_nqn} -a -s SPDK00000000000001 -m 12",
        )
        return subsystem_nqn

    def _add_namespaces(
        self,
        ssh: paramiko.SSHClient,
        spdk_path: str,
        subsystem_nqn: str,
        bdevs: List[str],
    ) -> None:
        """
        Add namespaces to the subsystem.
        """
        rpc_script = f"{spdk_path}/scripts/rpc.py"
        for bdev in bdevs:
            self.logger.info(f"Adding namespace {bdev} to subsystem {subsystem_nqn}")
            self._execute_command(
                ssh, f"{rpc_script} nvmf_subsystem_add_ns {subsystem_nqn} {bdev}"
            )

    def _add_listener(
        self, ssh: paramiko.SSHClient, spdk_path: str, subsystem_nqn: str, ip: str
    ) -> None:
        """
        Add a listener to the subsystem.
        """
        trtype = self.transport_options["trtype"]
        self.logger.info(f"Adding {trtype} listener on {ip}:4420")
        rpc_script = f"{spdk_path}/scripts/rpc.py"
        self._execute_command(
            ssh,
            f"{rpc_script} nvmf_subsystem_add_listener {subsystem_nqn} -t {shlex.quote(str(trtype))} -a {ip} -s 4420",
        )

    def deploy_target(self, target_config: Dict[str, Any]) -> bool:
        """
        Deploy SPDK target on a single node.
        """
        ip = target_config["ip"]
        spdk_path = target_config["path"]
        pci_devices = target_config["pci_devices"]

        self.logger.info(f"Deploying SPDK target on {ip}")

        try:
            # Establish SSH connection
            ssh = self._ssh_connect(ip, port=self.port)

            try:
                auto_discovered = not pci_devices
                if not pci_devices:
                    pci_devices = self._discover_nvme_pci_devices(ssh)
                else:
                    self.logger.info(
                        f"Using explicitly specified PCI devices for {ip}: {', '.join(pci_devices)}"
                    )

                # Setup SPDK
                self._setup_spdk(ssh, spdk_path, pci_devices)
                pci_devices = self._filter_spdk_ready_pci_devices(
                    ssh, pci_devices, strict=not auto_discovered
                )
                self.logger.info(
                    f"Target {ip} will expose PCI devices: {', '.join(pci_devices)}"
                )

                # Start tgt service
                # Modified By Yida (v6): 需要配 iobuf 池时以 --wait-for-rpc
                # 启动，在 startup 窗口内先配池子再 framework_start_init
                self._start_spdk_tgt(ssh, spdk_path, wait_for_rpc=bool(self.iobuf))
                self._wait_for_rpc_ready(ssh, spdk_path)
                if self.iobuf:
                    self._configure_iobuf(ssh, spdk_path)

                # Create transport
                self._create_transport(ssh, spdk_path)

                # Create bdevs
                bdevs = self._create_bdevs(ssh, spdk_path, pci_devices)

                # Create subsystem
                subsystem_nqn = self._create_subsystem(ssh, spdk_path)

                # Add namespaces
                self._add_namespaces(ssh, spdk_path, subsystem_nqn, bdevs)

                # Add listener
                self._add_listener(ssh, spdk_path, subsystem_nqn, ip)

                self.logger.info(f"Successfully deployed SPDK target on {ip}")
                return True
            finally:
                ssh.close()
        except Exception as e:
            self.logger.error(f"Failed to deploy SPDK target on {ip}: {e}")
            return False

    def deploy_all_targets(self) -> bool:
        """
        Deploy SPDK targets on all specified nodes.
        """
        success_count = 0
        total_count = len(self.target_configs)

        for i, target_config in enumerate(self.target_configs):
            self.logger.info(f"=== Deploying target {i+1}/{total_count} ===")
            if self.deploy_target(target_config):
                success_count += 1

        self.logger.info("=== Deployment Summary ===")
        self.logger.info(f"Total targets: {total_count}")
        self.logger.info(f"Successfully deployed: {success_count}")
        self.logger.info(f"Failed: {total_count - success_count}")

        return success_count == total_count


def parse_arguments():
    parser = argparse.ArgumentParser(description="SPDK Target Creator Tool")
    parser.add_argument(
        "--spdk_target_info",
        action="append",
        help=(
            'SPDK target information (e.g., "ip:192.168.65.56 '
            'path:/home/spdk pci:0000:01:00.0,0000:02:00.0"). If pci is '
            "omitted, SPDK-ready or unmounted NVMe devices on the target are used."
        ),
        required=True,
    )
    parser.add_argument('--core-mask', type=str, default='0xff',
                        help='CPU core mask used to start nvmf_tgt with -m (default: 0xff)')
    parser.add_argument('--transport-type', type=str, default='RDMA',
                        choices=['RDMA', 'TCP', 'URMA'],
                        help='NVMe-oF transport type for nvmf_create_transport (default: RDMA)')
    parser.add_argument('--max-queue-depth', type=int, default=128,
                        help='Max number of outstanding I/O per queue (default: 128)')
    parser.add_argument('--max-io-qpairs-per-ctrlr', type=int, default=127,
                        help='Max number of I/O qpairs per controller (default: 127)')
    # Modified By Yida (v3): 默认值与 SPDK 语义对齐（max_io_size=131072，
    # in_capsule_data_size=4096），与 DEFAULT_TRANSPORT_OPTIONS 保持一致
    parser.add_argument('--max-io-size', type=int, default=131072,
                        help='Max I/O size in bytes (default: 131072)')
    parser.add_argument('--in-capsule-data-size', type=int, default=4096,
                        help='Max in-capsule data size in bytes (default: 4096)')
    parser.add_argument('--io-unit-size', type=int, default=131072,
                        help='I/O unit size in bytes (default: 131072)')
    parser.add_argument('--max-aq-depth', type=int, default=128,
                        help='Max number of admin commands per admin queue (default: 128)')
    parser.add_argument('--num-shared-buffers', type=int, default=4096,
                        help='Number of pooled data buffers available to the transport (default: 4096)')
    parser.add_argument('--buf-cache-size', type=int, default=32,
                        help='Number of shared buffers reserved for each poll group (default: 32)')
    parser.add_argument('--username', type=str, default='root',
                        help='SSH username for target nodes (default: root)')
    parser.add_argument('--password', type=str,
                        help='SSH password for target nodes')
    parser.add_argument('--key-file', type=str,
                        help='SSH private key file path')
    # Modified By Yida (v3): 显式传入 nvmf_tgt 的 URMA 环境变量（可重复）。
    # 调用 shell 中已设置的 SPDK_URMA_* / MC_URMA_* 变量也会自动转发。
    parser.add_argument('--urma-env', action='append', default=None,
                        metavar='NAME=VALUE',
                        help='URMA environment variable to pass to nvmf_tgt, '
                             'e.g. --urma-env SPDK_URMA_DEV_NAME=udmac0d1e2 '
                             '(repeatable; SPDK_URMA_*/MC_URMA_* vars from the '
                             'invoking shell are forwarded automatically)')
    # Modified By Yida (v6): 大 I/O 部署（ADR-0017）。URMA 数据路径要求单
    # buffer（iovcnt==1），--max-io-size 超过默认 large_bufsize（135168）时
    # 必须重配 iobuf 池（nvmf_tgt 以 --wait-for-rpc 启动）
    parser.add_argument('--iobuf-options', type=str, default=None,
                        metavar='SMALL_COUNT,SMALL_BUF,LARGE_COUNT,LARGE_BUF',
                        help='iobuf pool four-tuple passed to iobuf_set_options '
                             'in the STARTUP window (starts nvmf_tgt with '
                             '--wait-for-rpc). Required for URMA max-io-size > '
                             '135168, e.g. "16384,8192,320,2097152"')
    parser.add_argument('--iobuf-large-cache-size', type=int, default=None,
                        help='Per-poll-group large iobuf cache for '
                             'nvmf_create_transport (default: 32 when '
                             '--iobuf-options is set). Must stay >= per-PG peak '
                             'concurrency, or the target-side URMA registration '
                             'cache hit rate collapses (ADR-0017 problem 3)')
    parser.add_argument('--port', type=int, default=22, help='SSH port (default: 22)')
    return parser.parse_args()


def main():
    args = parse_arguments()
    transport_options = {
        "trtype": args.transport_type,
        "max_queue_depth": args.max_queue_depth,
        "max_io_qpairs_per_ctrlr": args.max_io_qpairs_per_ctrlr,
        "max_io_size": args.max_io_size,
        "in_capsule_data_size": args.in_capsule_data_size,
        "io_unit_size": args.io_unit_size,
        "max_aq_depth": args.max_aq_depth,
        "num_shared_buffers": args.num_shared_buffers,
        "buf_cache_size": args.buf_cache_size,
    }

    # Modified By Yida (v3): 收集显式指定的 URMA 环境变量
    urma_env = {}
    for item in (args.urma_env or []):
        if '=' in item:
            name, value = item.split('=', 1)
            urma_env[name.strip()] = value.strip()
        else:
            logging.warning(f"Ignoring invalid --urma-env entry (expected NAME=VALUE): {item}")

    try:
        creator = SPDKTgtCreator(args.spdk_target_info, transport_options, args.core_mask,
                                 port=args.port, urma_env=urma_env,
                                 iobuf_options=args.iobuf_options,
                                 iobuf_large_cache_size=args.iobuf_large_cache_size)
        success = creator.deploy_all_targets()
        exit(0 if success else 1)
    except Exception as e:
        logging.error(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
