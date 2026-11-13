#!/bin/bash
# Copyright 2024 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Color definitions
GREEN="\033[0;32m"
BLUE="\033[0;34m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
NC="\033[0m" # No Color

# Configuration
REPO_ROOT=`pwd`
GITHUB_PROXY=${GITHUB_PROXY:-"https://github.com"}
GOVER=1.26.9
OS_RELEASE_FILE=${OS_RELEASE_FILE:-/etc/os-release}

# Function to print section headers
print_section() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

# Function to print success messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Function to print error messages and exit
print_error() {
    echo -e "${RED}✗ ERROR: $1${NC}"
    exit 1
}

# Function to check command success
check_success() {
    if [ $? -ne 0 ]; then
        print_error "$1"
    fi
}

# Detect ScaleFabric SHCA (shca-tools).
has_shca_tools() {
    if command -v dpkg-query >/dev/null 2>&1; then
        dpkg-query -W -f='${Status}' shca-tools 2>/dev/null | grep -q "install ok installed"
        return $?
    fi
    if command -v rpm >/dev/null 2>&1; then
        rpm -q shca-tools >/dev/null 2>&1
        return $?
    fi
    return 1
}

read_os_release_value() {
    local key="$1"
    awk -F= -v key="$key" '
        $1 == key {
            value = $0
            sub(/^[^=]*=/, "", value)
            gsub(/^"|"$/, "", value)
            print value
            exit
        }
    ' "$OS_RELEASE_FILE"
}

# Function to detect OS
detect_os() {
    if [ -f "$OS_RELEASE_FILE" ]; then
        ID=$(read_os_release_value ID)
        VERSION_ID=$(read_os_release_value VERSION_ID)
        OS=$(echo "$ID" | tr '[:upper:]' '[:lower:]')
        OS_VERSION=$VERSION_ID
    elif [ -f /etc/redhat-release ]; then
        OS="centos"
    else
        print_error "Cannot detect OS. Supported OS: Ubuntu, Debian, CentOS, RHEL, Rocky, AlmaLinux, EulerOS, openEuler, and Kylin."
    fi

    echo -e "${GREEN}Detected OS: $OS ${OS_VERSION:-unknown}${NC}"
}

if [ $(id -u) -ne 0 ]; then
	print_error "Require root permission, try sudo ./dependencies.sh"
fi

# Parse command line arguments
SKIP_CONFIRM=false
INSTALL_SPDK=false
# Modified By Yida (v3): NoF/URMA 支持——SPDK 来源与 URMA transport 开关。
# URMA 模式默认使用本项目 fork 的 SPDK，并固定到具体 commit（initiator/target
# 两端 wire 版本必须一致，只固定分支名不够）。
# Modified By Yida (v6): 默认引用升级到 urma_modified_v6（c44ccce27）。v6 与
# v3 wire 兼容，但两端仍统一 v6：hdr+capsule 合并发送、TCP_NODELAY、poller
# 批量排空、大 I/O 与 per-I/O trace 开关只在 v6 生效。
# Modified By Yida (urma_enable_v2): SE_Review v2 生命周期收敛 (da037fbc7) 与
# Round 7 v2 阶段遥测与 queued response 生命周期修复；wire v4，两端必须同版本部署。
# Modified By Senorita (urma_enable_v2 前移): qpair drain 收尾（retire posted
# receive refs）、split I/O qpair 容量、URMA/NVMe 读取尾段阶段遥测与
# urma_stage_buffer 统一缓冲处理（9b5cd59ca..e12b7cb21，共 5 提交）。
SPDK_URMA_REPO_DEFAULT="https://github.com/yyyuanhao426-hash/spdk.git"
SPDK_URMA_REF_DEFAULT="e12b7cb213a9c1a2a22b99ab4732c8c11f95032c" # urma_enable_v2
SPDK_REPO_URL=""
SPDK_REF="v23.01.1"
WITH_URMA=false
UMDK_ROOT=""
for arg in "$@"; do
    case $arg in
        -y|--yes)
            SKIP_CONFIRM=true
            ;;
        --with-spdk)
            INSTALL_SPDK=true
            ;;
        --spdk-repo=*)
            SPDK_REPO_URL="${arg#*=}"
            ;;
        --spdk-ref=*)
            SPDK_REF="${arg#*=}"
            ;;
        --with-urma)
            WITH_URMA=true
            ;;
        --with-urma=*)
            WITH_URMA=true
            UMDK_ROOT="${arg#*=}"
            ;;
        --umdk-root=*)
            WITH_URMA=true
            UMDK_ROOT="${arg#*=}"
            ;;
        -h|--help)
            echo -e "${YELLOW}Mooncake Dependencies Installer${NC}"
            echo -e "Usage: ./dependencies.sh [OPTIONS]"
            echo -e "\nOptions:"
            echo -e "  -y, --yes       Skip confirmation and install all dependencies"
            echo -e "  --with-spdk     Install SPDK for NVMe-oF support"
            echo -e "  --spdk-repo=URL Clone SPDK from the given repo instead of upstream"
            echo -e "                  (default with URMA: ${SPDK_URMA_REPO_DEFAULT})"
            echo -e "  --spdk-ref=REF  Checkout the given branch/tag/commit (default: ${SPDK_REF})"
            echo -e "                  (default with URMA: ${SPDK_URMA_REF_DEFAULT})"
            echo -e "  --with-urma[=DIR] Configure SPDK with the URMA transport."
            echo -e "                  DIR may point to a UMDK install prefix (defaults to"
            echo -e "                  \${UMDK_ROOT} or system paths)"
            echo -e "  --umdk-root=DIR Alias of --with-urma=DIR"
            echo -e "  -h, --help      Show this help message and exit"
            exit 0
            ;;
    esac
done

if [ "$WITH_URMA" = true ]; then
    if [ -z "$SPDK_REPO_URL" ]; then
        SPDK_REPO_URL="${SPDK_URMA_REPO_DEFAULT}"
    fi
    if [ "$SPDK_REF" = "v23.01.1" ]; then
        SPDK_REF="${SPDK_URMA_REF_DEFAULT}"
    fi
fi

# Print welcome message
echo -e "${YELLOW}Mooncake Dependencies Installer${NC}"
echo -e "This script will install all required dependencies for Mooncake."
echo -e "The following components will be installed:"
echo -e "  - System packages (build tools, libraries)"
echo -e "  - Git submodules (pybind11)"
echo -e "  - Go $GOVER"
if [ "$INSTALL_SPDK" = true ]; then
    echo -e "  - SPDK (for NVMe-oF support)"
fi
echo

# Ask for confirmation unless -y flag is used
if [ "$SKIP_CONFIRM" = false ]; then
    read -p "Do you want to continue? [Y/n] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]] && [[ ! $REPLY = "" ]]; then
        echo -e "${YELLOW}Installation cancelled.${NC}"
        exit 0
    fi
fi

# Detect OS
detect_os

# Update package lists
print_section "Updating package lists"
if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
    apt-get update
    check_success "Failed to update package lists"
elif [ "$OS" = "centos" ] || [ "$OS" = "rhel" ] || [ "$OS" = "rocky" ] || [ "$OS" = "almalinux" ] || [ "$OS" = "euleros" ] || [ "$OS" = "openeuler" ] || [ "$OS" = "kylin" ]; then
    yum install -y dnf-plugins-core epel-release || true
    yum config-manager --set-enabled powertools || yum config-manager --set-enabled crb || true
    yum clean all
    yum makecache
    check_success "Failed to update package lists"
else
    print_error "Unsupported OS: $OS"
fi

# Install system packages
print_section "Installing system packages"
echo -e "${YELLOW}This may take a few minutes...${NC}"

if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
    SYSTEM_PACKAGES="build-essential \
                     cmake \
                     ninja-build \
                     git \
                     wget \
                     unzip \
                     libibverbs-dev \
                     libgoogle-glog-dev \
                     libjsoncpp-dev \
                     libunwind-dev \
                     libnuma-dev \
                     libpython3-dev \
                     libboost-all-dev \
                     libssl-dev \
                     libgrpc-dev \
                     libgrpc++-dev \
                     libprotobuf-dev \
                     libyaml-cpp-dev \
                     protobuf-compiler-grpc \
                     libcurl4-openssl-dev \
                     libhiredis-dev \
                     liburing-dev \
                     libjemalloc-dev \
                     libmsgpack-dev \
                     libzmq3-dev \
                     libzstd-dev \
                     libasio-dev \
                     libxxhash-dev \
                     libcli11-dev \
                     pkg-config \
                     patchelf \
                     libc6-dev \
                     libc-bin"

    # ScaleFabric SHCA (shca-tools) ships its own libibverbs headers/libs; installing
    # libibverbs-dev conflicts with it. libboost-all-dev pulls OpenMPI/libfabric,
    # which also depend on distro ibverbs and fail on SHCA systems.
    if has_shca_tools; then
        SYSTEM_PACKAGES=$(echo $SYSTEM_PACKAGES | sed 's/libibverbs-dev//g')
        SYSTEM_PACKAGES=$(echo $SYSTEM_PACKAGES | sed "s/libboost-all-dev/libboost-dev/g")
        echo -e "${GREEN}shca-tools package detected. Adjusting system packages accordingly; build with -DUSE_SHCA=ON to enable SHCA support.${NC}"
    fi

    # Since Ubuntu 26.04, libmsgpack-dev is a transitional package for the C-only
    # libmsgpack-c-dev, so msgpack.hpp needs libmsgpack-cxx-dev. Ubuntu 22.04 has no
    # libmsgpack-cxx-dev and ships msgpack.hpp in libmsgpack-dev.
    if apt-cache show libmsgpack-cxx-dev > /dev/null 2>&1; then
        SYSTEM_PACKAGES="$SYSTEM_PACKAGES libmsgpack-cxx-dev"
    fi

    apt-get install -y $SYSTEM_PACKAGES
    check_success "Failed to install system packages"

elif [ "$OS" = "centos" ] || [ "$OS" = "rhel" ] || [ "$OS" = "rocky" ] || [ "$OS" = "almalinux" ] || [ "$OS" = "euleros" ] || [ "$OS" = "openeuler" ]; then
    SYSTEM_PACKAGES="@development \
                     cmake \
                     ninja-build \
                     git \
                     wget \
                     rdma-core-devel \
                     glog-devel \
                     gflags-devel \
                     jsoncpp-devel \
                     libunwind-devel \
                     numactl-devel \
                     python3-devel \
                     boost1.78-devel \
                     openssl-devel \
                     protobuf-devel \
                     yaml-cpp-devel \
                     libcurl-devel \
                     hiredis-devel \
                     liburing-devel \
                     jemalloc-devel \
                     msgpack-devel \
                     zeromq-devel \
                     libzstd-devel \
                     pkgconf-pkg-config \
                     elfutils-libelf-devel \
                     patchelf  \
                     xxhash-devel \
                     libbsd-devel"

    # Same SHCA conflict on RHEL-family: skip rdma-core-devel when shca-tools is present.
    if has_shca_tools; then
        SYSTEM_PACKAGES=$(echo $SYSTEM_PACKAGES | sed 's/rdma-core-devel//g')
        echo -e "${GREEN}shca-tools package detected. Skipping rdma-core-devel (provided by shca-tools).${NC}"
    fi

    yum install -y $SYSTEM_PACKAGES
    check_success "Failed to install system packages"
    # The TENT CLI needs CLI11 headers; some releases do not package them.
    yum install -y cli11-devel || echo -e "${YELLOW}cli11-devel is not available; install CLI11 headers manually to build the TENT CLI.${NC}"

elif [ "$OS" = "kylin" ]; then
    SYSTEM_PACKAGES="@development \
                     cmake \
                     ninja-build \
                     git \
                     wget \
                     rdma-core-devel \
                     glog-devel \
                     gflags-devel \
                     jsoncpp-devel \
                     libunwind-devel \
                     numactl-devel \
                     python3-devel \
                     boost-devel \
                     openssl-devel \
                     protobuf-devel \
                     yaml-cpp-devel \
                     libcurl-devel \
                     hiredis-devel \
                     liburing-devel \
                     jemalloc-devel \
                     msgpack-devel \
                     libzstd-devel \
                     pkgconf-pkg-config \
                     elfutils-libelf-devel \
                     patchelf  \
                     xxhash-devel \
                     libbsd-devel"

    if [ -z "${KYLIN_EPKL_URL:-}" ]; then
        KYLIN_NKVERS_OUTPUT=""
        if command -v nkvers >/dev/null 2>&1; then
            KYLIN_NKVERS_OUTPUT=$(nkvers 2>/dev/null)
        fi

        KYLIN_VERSION=${KYLIN_VERSION:-$(printf '%s\n' "$KYLIN_NKVERS_OUTPUT" | sed -nE 's/.*release[[:space:]]+(V[0-9]+).*/\1/p' | head -n 1)}
        KYLIN_VERSION=${KYLIN_VERSION:-$OS_VERSION}
        case "$KYLIN_VERSION" in
            V*) ;;
            [0-9]*) KYLIN_VERSION="V${KYLIN_VERSION}" ;;
        esac

        KYLIN_EPKL_RELEASE=${KYLIN_EPKL_RELEASE:-$(printf '%s\n' "$KYLIN_NKVERS_OUTPUT" | sed -nE 's/.*release[[:space:]]+V[0-9]+[[:space:]]+([0-9]{4})\/.*/\1/p' | head -n 1)}
        if [ -z "$KYLIN_VERSION" ] || [ -z "$KYLIN_EPKL_RELEASE" ]; then
            print_error "Cannot detect Kylin release. Set KYLIN_EPKL_URL or KYLIN_EPKL_RELEASE explicitly."
        fi

        KYLIN_EPKL_URL="https://eps-server.openkylin.top/NS/${KYLIN_VERSION}/${KYLIN_EPKL_RELEASE}/EPKL/main/$(uname -m)/"
    fi
    dnf --repofrompath="kylin-epkl,$KYLIN_EPKL_URL" \
        --setopt=kylin-epkl.gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-kylin \
        --enablerepo=kylin-epkl install -y $SYSTEM_PACKAGES
    check_success "Failed to install system packages"
    dnf --repofrompath="kylin-epkl,$KYLIN_EPKL_URL" \
        --setopt=kylin-epkl.gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-kylin \
        --enablerepo=kylin-epkl install -y cli11-devel || echo -e "${YELLOW}cli11-devel is not available; install CLI11 headers manually to build the TENT CLI.${NC}"
else
    print_error "Unsupported OS: $OS"
fi

print_success "System packages installed successfully"

# Initialize and update git submodules
print_section "Initializing Git Submodules"

# Check if .gitmodules exists
if [ -f "${REPO_ROOT}/.gitmodules" ]; then
    echo "Enter repository root: ${REPO_ROOT}"
    cd "${REPO_ROOT}"
    check_success "Failed to change to repository root directory"

    echo "Initializing git submodules..."
    git submodule sync --recursive
    check_success "Failed to sync git submodules"
    git submodule update --init --recursive
    check_success "Failed to initialize git submodules"

    print_success "Git submodules initialized and updated successfully"
else
    echo -e "${YELLOW}No .gitmodules file found. Skipping...${NC}"
    exit 1
fi

print_section "Verifying essential build tools"

# Verify getconf and ldd (required for glibc version detection in build_wheel.sh)
if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
    if ! command -v getconf >/dev/null 2>&1; then
        print_error "getconf not found after installing system packages. This should not happen."
    fi
    if ! command -v ldd >/dev/null 2>&1; then
        print_error "ldd not found after installing system packages. This should not happen."
    fi
    print_success "getconf found: $(getconf --version 2>&1 | head -1)"
    print_success "ldd found: $(ldd --version 2>&1 | head -1)"
fi

print_section "Installing Go $GOVER"

USED_CN_MIRROR=false

install_go() {
    ARCH=$(uname -m)
    if [ "$ARCH" = "aarch64" ]; then
        ARCH="arm64"
    elif [ "$ARCH" = "x86_64" ]; then
        ARCH="amd64"
    else
        echo "Unsupported architecture: $ARCH"
        exit 1
    fi

    GO_TARBALL="go$GOVER.linux-$ARCH.tar.gz"

    # Try multiple download mirrors with fallback
    GO_DOWNLOAD_URLS=(
        "https://go.dev/dl/${GO_TARBALL}"
        "https://golang.google.cn/dl/${GO_TARBALL}"
        "https://mirrors.aliyun.com/golang/${GO_TARBALL}"
    )

    DOWNLOAD_SUCCESS=false
    for url in "${GO_DOWNLOAD_URLS[@]}"; do
        echo "Downloading Go $GOVER from ${url}..."
        if wget -q --show-progress --timeout=30 --tries=2 -O "${GO_TARBALL}" "${url}"; then
            DOWNLOAD_SUCCESS=true
            if [[ "$url" != "https://go.dev/dl/${GO_TARBALL}" ]]; then
                USED_CN_MIRROR=true
            fi
            print_success "Downloaded Go $GOVER from ${url}"
            break
        else
            echo -e "${YELLOW}Failed to download from ${url}, trying next mirror...${NC}"
            rm -f "${GO_TARBALL}"
        fi
    done

    if [ "$DOWNLOAD_SUCCESS" = false ]; then
        print_error "Failed to download Go $GOVER from all mirrors"
    fi

    echo "Installing Go $GOVER..."
    tar -C /usr/local -xzf "${GO_TARBALL}"
    check_success "Failed to install Go $GOVER"

    rm -f "${GO_TARBALL}"
    check_success "Failed to clean up Go installation file"

    print_success "Go $GOVER installed successfully"
}

if command -v go &> /dev/null; then
    GO_VERSION=$(go version | awk '{print $3}')
    if [[ "$GO_VERSION" == "go$GOVER" ]]; then
        echo -e "${YELLOW}Go $GOVER is already installed. Skipping...${NC}"
    else
        echo -e "${YELLOW}Found Go $GO_VERSION. Will install Go $GOVER...${NC}"
        install_go
    fi
else
    install_go
fi

# Add Go to PATH if not already there
if ! grep -q "export PATH=\$PATH:/usr/local/go/bin" ~/.bashrc; then
    echo -e "${YELLOW}Adding Go to your PATH in ~/.bashrc${NC}"
    echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
    echo -e "${YELLOW}Please run 'source ~/.bashrc' or start a new terminal to use Go${NC}"
fi

# Set GOPROXY only if Go download fell back to a CN mirror
if [ "$USED_CN_MIRROR" = true ] && [ -z "$GOPROXY" ]; then
    export GOPROXY=https://goproxy.cn,https://goproxy.io,direct
    echo -e "${YELLOW}Detected restricted network (Go was downloaded from a CN mirror).${NC}"
    echo -e "${YELLOW}GOPROXY set to: ${GOPROXY}${NC}"
    if ! grep -q "export GOPROXY=" ~/.bashrc; then
        echo 'export GOPROXY=https://goproxy.cn,https://goproxy.io,direct' >> ~/.bashrc
        echo -e "${YELLOW}GOPROXY added to ~/.bashrc for future sessions${NC}"
    fi
elif [ -n "$GOPROXY" ]; then
    echo -e "${GREEN}GOPROXY already set to: ${GOPROXY}${NC}"
fi

# Install SPDK if requested
if [ "$INSTALL_SPDK" = true ]; then
    print_section "Installing SPDK"

    if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
        apt-get install -y libelf-dev
        check_success "Failed to install NoF dependencies"
    fi

    cd "${REPO_ROOT}/extern"
    check_success "Failed to change to extern directory"

    # Remove existing SPDK if present
    if [ -d "spdk" ]; then
        echo -e "${YELLOW}SPDK directory already exists. Removing for fresh install...${NC}"
        rm -rf spdk
        check_success "Failed to remove existing SPDK directory"
    fi

    # Clone SPDK
    # Modified By Yida (v3): SPDK 来源与版本可通过 --spdk-repo/--spdk-ref 覆盖，
    # URMA 模式默认克隆本项目 fork 并固定 commit。
    if [ -z "$SPDK_REPO_URL" ]; then
        SPDK_REPO_URL="${GITHUB_PROXY}/spdk/spdk.git"
    fi
    echo "Cloning SPDK from ${SPDK_REPO_URL}..."
    git clone "${SPDK_REPO_URL}" spdk
    check_success "Failed to clone SPDK"

    cd spdk
    check_success "Failed to change to SPDK directory"

    # Checkout specific version
    echo "Checking out SPDK version ${SPDK_REF}..."
    git checkout "${SPDK_REF}"
    check_success "Failed to checkout SPDK version ${SPDK_REF}"

    # Initialize submodules
    echo "Initializing SPDK submodules..."
    git submodule update --init
    check_success "Failed to initialize SPDK submodules"

    # Install SPDK dependencies
    echo "Installing SPDK dependencies..."
    ./scripts/pkgdep.sh --rdma
    check_success "Failed to install SPDK RDMA dependencies"

    # Configure SPDK with RDMA support
    # Modified By Yida (v3): --with-urma 启用 URMA transport；DIR 指向 UMDK
    # 安装前缀（缺省时依赖系统路径）。URMA 模式下目标节点必须先装好 UMDK。
    echo "Configuring SPDK with RDMA support..."
    if [ "$WITH_URMA" = true ]; then
        if [ -n "$UMDK_ROOT" ]; then
            ./configure --with-rdma --with-urma="$UMDK_ROOT"
        else
            ./configure --with-rdma --with-urma
        fi
        check_success "Failed to configure SPDK (with URMA; ensure UMDK is installed)"
    else
        ./configure --with-rdma
        check_success "Failed to configure SPDK"
    fi

    # Build SPDK
    echo "Building SPDK (using $(nproc) cores)..."
    make -j$(nproc)
    check_success "Failed to build SPDK"

    # Install SPDK
    # Modified By Yida (v6): SPDK 26.x 的 install 链末尾用 uv pip 打包
    # spdk/python（PEP 517 需在线拉 hatchling）。出口受限或 sudo 剥离代理 env
    # 时该步骤报 Error 2，但此时静态库与头文件已全部装完——Mooncake 只依赖
    # /usr/local 下的库与头文件（ADR-0018）。因此退出码只作参考，判定收敛
    # 为产物完整性检查。
    echo "Installing SPDK..."
    if ! make install; then
        echo -e "${YELLOW}make install exited nonzero (usually the spdk/python hatchling step); core artifacts may already be installed.${NC}"
    fi

    INSTALL_INCOMPLETE=""
    for _f in libspdk_event.a libspdk_nvme.a; do
        if [ ! -f "/usr/local/lib/${_f}" ]; then
            echo -e "${RED}NOT INSTALLED: /usr/local/lib/${_f}${NC}"
            INSTALL_INCOMPLETE=1
        fi
    done
    if [ -n "$INSTALL_INCOMPLETE" ]; then
        print_error "SPDK install incomplete. Rerun 'sudo -E make install' in $(pwd) (sudo -E keeps proxy env for the hatchling step; see ADR-0018)"
    fi
    print_success "SPDK core static libraries verified in /usr/local/lib"

    # Copy DPDK libraries to system library path
    if ls dpdk/build/lib/*.a >/dev/null 2>&1; then
        echo "Copying DPDK libraries to /usr/local/lib..."
        cp dpdk/build/lib/*.a /usr/local/lib/
        check_success "Failed to copy DPDK libraries"
    fi

    print_success "SPDK installed successfully"
    cd "${REPO_ROOT}"
fi

# Return to the repository root
cd "${REPO_ROOT}"

# Print summary
print_section "Installation Complete"
echo -e "${GREEN}All dependencies have been successfully installed!${NC}"
echo -e "The following components were installed:"
echo -e "  ${GREEN}✓${NC} System packages"
echo -e "  ${GREEN}✓${NC} Git submodules"
echo -e "  ${GREEN}✓${NC} Go $GOVER"
if [ "$INSTALL_SPDK" = true ]; then
    if [ "$WITH_URMA" = true ]; then
        echo -e "  ${GREEN}✓${NC} SPDK (${SPDK_REF}, URMA transport enabled)"
    else
        echo -e "  ${GREEN}✓${NC} SPDK (${SPDK_REF})"
    fi
fi
echo
echo -e "You can now build and run Mooncake."
echo -e "${YELLOW}Note: You may need to restart your terminal or run 'source ~/.bashrc' to use Go.${NC}"

if [ "$INSTALL_SPDK" = true ]; then
    echo -e "${YELLOW}Note: SPDK requires hugepages and RDMA configuration. Please refer to SPDK documentation for setup.${NC}"
fi
