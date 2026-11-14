#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# Mooncake 930 分支（nof-gdr-supercache-snapshot-930）编译 + 安装 + 分发
#
# 与 build_mooncake_yh.sh 的差异（该文件保留未改）：
#   [1] 所有路径可指定，不再写死 /home/yin：
#         --root DIR    工作根目录（默认 $HOME）
#         --src DIR     源码目录（默认 <root>/nof/mooncake-930）
#         --build-dir DIR 构建目录（默认 <src>/build）
#         --spdk DIR    SPDK 源码树（默认 <root>/nof/spdk）
#         --venv DIR    python venv（默认 <root>/venvs/mooncake-bench，不存在则自动创建）
#         --umdk DIR    UMDK 前缀（默认自动探测；含 include/ub/umdk/urma + lib/liburma.so）
#         --prefix DIR  安装前缀（默认 /usr/local；给了 --root 则默认 <root>/mooncake-930-install）
#         --nodes "a b" 编译后 scp 分发（默认空=只本机）
#   [2] 新增 make install（原脚本只收集到 outputs/，/usr/local/bin 不会更新）
#   [3] venv 不存在时自动创建并装 aiohttp/paramiko（原脚本直接报错退出）
#   [4] USE_ETCD 默认 OFF（原脚本强制 ON，会触发 go mod tidy 联网构建 etcd_wrapper）；
#       需要时 --etcd 打开。libetcd_wrapper.so 缺失不再中断脚本
#   [5] wheel 默认不构建（cmake install 已把 store.so 装进 venv；wheel+auditwheel 链路
#       在本环境从未跑通）。需要时 --with-wheel；--nodes 分发时自动要求 wheel
#   [6] SPDK 自检失败时 --install-spdk 直接编译（带 --disable-unit-tests，规避
#       e12b7cb21+ 的 spdk_get_ticks 链接回归），不再调用 dependencies.sh
#       （后者会 rm -rf extern/spdk 且不带该 flag）
#   [7] 代理可选：默认探测 127.0.0.1:3128，不通则跳过；--no-proxy 强制关闭
#   [8] 预检：UMDK/SPDK 路径对当前用户不可读时直接给出处置命令（/home/* 常是 0700）
#   [9] 远端重装 wheel 用远端 $HOME 推导 venv，不再把本机绝对路径带到远端
#  [10] 修正 -lurma 解析：yalantinglibs 的 YLT_ENABLE_URMA 传播裸 "-lurma"，
#       默认会命中 /usr/lib64/liburma.so（旧版，缺 urma_register_seg_dmabuf）→
#       mooncake_master 链接失败。脚本把 UMDK lib 目录前置到链接搜索路径。
#  [11] 构建锁放在 build 目录，不往源码树写文件
#
# 用法：
#   bash build_mooncake_930.sh --help
#   bash build_mooncake_930.sh                       # 编译+安装到本机
#   bash build_mooncake_930.sh --check               # 只做预检
#   bash build_mooncake_930.sh --root /home/tong     # 源码/venv/安装全落到 /home/tong
#   bash build_mooncake_930.sh --root /home/tong --prefix /home/tong/mc930   # 单独指定安装目录
#   bash build_mooncake_930.sh --nodes "node2 node3" --with-wheel
#
# 运行期仍需（部署/冒烟时）：
#   export LD_LIBRARY_PATH=<umdk>/lib:/usr/local/lib64:/usr/local/lib
###############################################################################

# ===================== 默认配置（均可用参数/环境变量覆盖） =====================
WORK_ROOT="${WORK_ROOT:-$HOME}"
SRC_ROOT="${SRC_ROOT:-$WORK_ROOT/nof/mooncake-930}"
SPDK_SRC_DIR="${SPDK_SRC_DIR:-$WORK_ROOT/nof/spdk}"
BUILD_DIR="${BUILD_DIR:-}"
VENV="${VENV:-$WORK_ROOT/venvs/mooncake-bench}"
INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local}"
INSTALL_EXPLICIT=0          # --prefix 显式给过则 --root 不再推导安装前缀
UMDK_PREFIX="${UMDK_PREFIX:-}"
URMA_ROOT="${URMA_ROOT:-/usr/include/ub/umdk/urma}"

GIT_REPO="${GIT_REPO:-https://github.com/LeeJace993/Mooncake.git}"
GIT_BRANCH="${GIT_BRANCH:-nof-gdr-supercache-snapshot-930}"
GIT_COMMIT="${GIT_COMMIT:-706551a8}"
YLT_PIN="${YLT_PIN:-e46cbf08}"

SPDK_REPO="${SPDK_REPO:-https://github.com/yyyuanhao426-hash/spdk.git}"
SPDK_COMMIT="${SPDK_COMMIT:-e12b7cb213a9c1a2a22b99ab4732c8c11f95032c}"

PATCH_930="${PATCH_930:-true}"
USE_ETCD="${USE_ETCD:-OFF}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
JOBS="${JOBS:-$(nproc)}"
CLEAN_BUILD="${CLEAN_BUILD:-false}"
WITH_WHEEL="${WITH_WHEEL:-false}"
INSTALL_SPDK="${INSTALL_SPDK:-false}"
CHECK_ONLY="${CHECK_ONLY:-false}"
PROXY_MODE="${PROXY_MODE:-auto}"        # auto | on | off
PROXY_URL="${PROXY_URL:-http://127.0.0.1:3128}"
NODES_STR="${NODES_STR:-}"
REMOTE_VENV="${REMOTE_VENV:-\$HOME/venvs/mooncake-bench}"

BIN_TARGETS=(mooncake_master mooncake_client nof_register nof_unregister stress_cluster_bench)
SO_TARGETS=(libasio.so libetcd_wrapper.so)

log()  { echo -e "\n\033[1;36m[$(date '+%H:%M:%S')] $*\033[0m"; }
ok()   { echo -e "  \033[1;32m[OK]\033[0m $*"; }
warn() { echo -e "  \033[1;33m[WARN]\033[0m $*"; }
die()  { echo -e "  \033[1;31m[ERROR]\033[0m $*" >&2; exit 1; }

usage() { sed -n '2,42p' "$0" | sed 's/^# \?//'; exit 0; }

# ===================== 参数解析（--opt=V 与 --opt V 均可） =====================
# optval NAME：取当前参数的值。--opt=V 直接切；--opt 则吃掉下一个参数。
optval() {
    case "$1" in
        *=*) echo "${1#*=}";;
        *)   [ $# -ge 2 ] || die "$1 需要取值"; echo "$2";;
    esac
}
while [ $# -gt 0 ]; do
    arg="$1"
    case "$arg" in
        --root|--root=*)
            WORK_ROOT=$(optval "$@"); SRC_ROOT="$WORK_ROOT/nof/mooncake-930"
            SPDK_SRC_DIR="$WORK_ROOT/nof/spdk"; VENV="$WORK_ROOT/venvs/mooncake-bench"
            # --prefix 显式给过就尊重它（不论参数先后）
            [ "$INSTALL_EXPLICIT" = 1 ] || INSTALL_PREFIX="$WORK_ROOT/mooncake-930-install"
            [ "$arg" = "--root" ] && shift;;
        --src|--src=*)         SRC_ROOT=$(optval "$@");      [ "$arg" = "--src" ] && shift;;
        --spdk|--spdk=*)       SPDK_SRC_DIR=$(optval "$@");  [ "$arg" = "--spdk" ] && shift;;
        --build-dir|--build-dir=*) BUILD_DIR=$(optval "$@"); [ "$arg" = "--build-dir" ] && shift;;
        --venv|--venv=*)       VENV=$(optval "$@");          [ "$arg" = "--venv" ] && shift;;
        --umdk|--umdk=*)       UMDK_PREFIX=$(optval "$@");   [ "$arg" = "--umdk" ] && shift;;
        --prefix|--prefix=*)   INSTALL_PREFIX=$(optval "$@"); INSTALL_EXPLICIT=1
                               [ "$arg" = "--prefix" ] && shift;;
        --nodes|--nodes=*)     NODES_STR=$(optval "$@");     [ "$arg" = "--nodes" ] && shift;;
        --jobs|--jobs=*)       JOBS=$(optval "$@");          [ "$arg" = "--jobs" ] && shift;;
        --build-type|--build-type=*) BUILD_TYPE=$(optval "$@"); [ "$arg" = "--build-type" ] && shift;;
        --remote-venv|--remote-venv=*) REMOTE_VENV=$(optval "$@"); [ "$arg" = "--remote-venv" ] && shift;;
        --proxy|--proxy=*)     PROXY_URL=$(optval "$@");     [ "$arg" = "--proxy" ] && shift;;
        --no-proxy)      PROXY_MODE=off;;
        --clean)         CLEAN_BUILD=true;;
        --etcd)          USE_ETCD=ON;;
        --with-wheel)    WITH_WHEEL=true;;
        --no-patch)      PATCH_930=false;;
        --install-spdk)  INSTALL_SPDK=true;;
        --check)         CHECK_ONLY=true;;
        --help|-h)       usage;;
        *)               die "未知参数: $arg（见 --help）";;
    esac
    shift
done
read -r -a NODES <<< "${NODES_STR}"

[ "$WITH_WHEEL" = "false" ] && [ "${#NODES[@]}" -gt 0 ] \
    && die "--nodes 分发需要 wheel（远端没有编译产物），请加 --with-wheel"

# ===================== 代理（可选） =====================
if [ "$PROXY_MODE" = "auto" ]; then
    if (exec 3<>/dev/tcp/127.0.0.1/3128) 2>/dev/null; then PROXY_MODE=on; else PROXY_MODE=off; fi
fi
if [ "$PROXY_MODE" = "on" ]; then
    export http_proxy="$PROXY_URL" https_proxy="$PROXY_URL"
    export no_proxy="192.168.0.0/16,141.0.0.0/8,10.0.0.0/8,172.16.0.0/12,127.0.0.1,localhost,local,.local"
    ok "proxy: $PROXY_URL"
else
    unset http_proxy https_proxy 2>/dev/null || true
    ok "proxy: off"
fi

# ===================== UMDK 探测 =====================
# 目标：找到同时满足 (a) 有 liburma.so 且导出 urma_register_seg_dmabuf
#       (b) 有 urma_api.h（优先前缀内，其次系统 /usr/include/ub/umdk/urma）
probe_umdk() {
    local cands=() c lib inc
    [ -n "$UMDK_PREFIX" ] && cands+=("$UMDK_PREFIX")
    cands+=(
        "$WORK_ROOT/gdr/UMDK_tool_netlab-sp4_umdk"
        "$WORK_ROOT/gdr/UMDK_netlab"
        /usr/local/umdk /usr/local/gdr-umdk /opt/umdk /usr
    )
    for c in "${cands[@]}"; do
        [ -d "$c" ] || continue
        lib=""
        for d in "$c/lib64" "$c/lib" "$c/build/lib" "$c/build/urma/lib/urma/core"; do
            if [ -e "$d/liburma.so" ]; then lib="$d/liburma.so"; break; fi
        done
        [ -n "$lib" ] || continue
        # 可读性 + 符号
        if ! head -c 4 "$lib" >/dev/null 2>&1; then
            warn "不可读（权限）: $lib"; continue
        fi
        if ! nm -D "$lib" 2>/dev/null | grep -q urma_register_seg_dmabuf; then
            warn "缺 urma_register_seg_dmabuf: $lib"; continue
        fi
        inc=""
        for d in "$c/include/ub/umdk/urma" "$c/include/ub/umdk" "$c/include" \
                 "$URMA_ROOT" /usr/include/ub/umdk/urma; do
            if [ -f "$d/urma_api.h" ]; then inc="$d"; break; fi
        done
        [ -n "$inc" ] || { warn "找不到 urma_api.h（前缀 $c）"; continue; }
        UMDK_LIB="$lib"; UMDK_INC="$inc"
        # UMDK_ROOT 仅在头文件位于该前缀内时给出，避免 cmake 指到空 include
        case "$inc" in
            "$c"/*) UMDK_ROOT="$c";;
            *)      UMDK_ROOT="";;
        esac
        return 0
    done
    return 1
}

log "环境预检"
echo "  user        = $(id -un) (uid=$(id -u))"
echo "  WORK_ROOT   = $WORK_ROOT"
echo "  SRC_ROOT    = $SRC_ROOT"
echo "  SPDK_SRC    = $SPDK_SRC_DIR"
echo "  VENV        = $VENV"
echo "  INSTALL     = $INSTALL_PREFIX"
echo "  BUILD_TYPE  = $BUILD_TYPE   JOBS=$JOBS   ETCD=$USE_ETCD   WHEEL=$WITH_WHEEL"

UMDK_LIB=""; UMDK_INC=""; UMDK_ROOT=""
if ! probe_umdk; then
    cat >&2 <<EOF

[ERROR] 没找到可用的 liburma.so（需导出 urma_register_seg_dmabuf）。
系统 /usr/lib64/liburma.so 是旧版，缺该符号，不能用于 930 分支。

处置（任选其一，需 root）：
  # 方案 A：把一份可用的 UMDK 放到公共可读目录（推荐）
  sudo mkdir -p /usr/local/umdk/lib /usr/local/umdk/include/ub/umdk
  sudo cp -a <可用的umdk>/lib/liburma.so* /usr/local/umdk/lib/
  sudo cp -a /usr/include/ub/umdk/urma /usr/local/umdk/include/ub/umdk/
  sudo chmod -R a+rX /usr/local/umdk
  bash $0 --umdk /usr/local/umdk

  # 方案 B：本机已有 /usr/local/gdr-umdk（内容与 round11 用的那份一致），放开读权限
  sudo chmod -R a+rX /usr/local/gdr-umdk
  bash $0

  # 方案 C：显式指定已有前缀
  bash $0 --umdk /path/to/umdk-prefix

注意：/home/<别人>/... 下的 UMDK 通常是 0700，其他用户读不到，不能直接用。
EOF
    exit 1
fi
ok "URMA_LIB=$UMDK_LIB"
ok "URMA_INC=$UMDK_INC   UMDK_ROOT=${UMDK_ROOT:-<none>}"
echo "  URMA_ROOT=$URMA_ROOT"

# ===================== 前置检查（工具/权限/SPDK） =====================
need() { command -v "$1" >/dev/null 2>&1 || die "缺少命令: $1"; }
for c in git cmake make nm flock python3 tar; do need "$c"; done
python3 -m venv --help >/dev/null 2>&1 || die "python3 缺 venv 模块（装 python3-venv）"

SPDK_OK=1
[ -f /usr/local/include/spdk/nvme_urma.h ] || SPDK_OK=0
[ -f /usr/local/lib/libspdk_nvme.a ] || SPDK_OK=0
SPDK_DIR_PROBED=""
for d in "$SPDK_SRC_DIR" "$SRC_ROOT/extern/spdk"; do
    [ -d "$d/.git" ] && { SPDK_DIR_PROBED="$d"; break; }
done
if [ -n "$SPDK_DIR_PROBED" ]; then
    SPDK_HEAD=$(git -C "$SPDK_DIR_PROBED" log --oneline -1 2>/dev/null || echo "<unreadable>")
    echo "  SPDK source = $SPDK_DIR_PROBED @ $SPDK_HEAD"
    echo "$SPDK_HEAD" | grep -q "${SPDK_COMMIT:0:9}" || SPDK_OK=0
else
    warn "SPDK 源码树未找到（$SPDK_SRC_DIR）；仅影响自检，不影响使用已安装的 /usr/local"
    SPDK_OK=0
fi
if [ "$SPDK_OK" = 1 ]; then
    ok "SPDK 自检通过（${SPDK_COMMIT:0:9}）"
else
    warn "SPDK 自检未通过（需 nvme_urma.h + libspdk_nvme.a + 源码 ${SPDK_COMMIT:0:9}）"
fi

if [ "$CHECK_ONLY" = "true" ]; then
    log "预检结束（--check）"; exit 0
fi

# ===================== 源码 =====================
log "源码：钉版 clone / 校验"
if [ ! -d "$SRC_ROOT/.git" ]; then
    [ -d "$SRC_ROOT" ] && [ -n "$(ls -A "$SRC_ROOT" 2>/dev/null)" ] \
        && die "$SRC_ROOT 已存在且非空、又不是 git 仓库；请清空或换 --src"
    echo ">> clone $GIT_REPO @ $GIT_BRANCH -> $SRC_ROOT"
    mkdir -p "$(dirname "$SRC_ROOT")"
    git clone --branch "$GIT_BRANCH" --recurse-submodules "$GIT_REPO" "$SRC_ROOT"
fi
cd "$SRC_ROOT"
git submodule update --init --recursive

HEAD_LINE=$(git log --oneline -1)
echo ">> HEAD: $HEAD_LINE"
echo "$HEAD_LINE" | grep -q "$GIT_COMMIT" \
    || warn "HEAD 不是期望的 $GIT_COMMIT（如需钉版：git -C $SRC_ROOT checkout $GIT_COMMIT）"

# yalantinglibs pin：用祖先判断，避免只看 HEAD 一行造成假告警
if [ -d extern/yalantinglibs/.git ]; then
    if git -C extern/yalantinglibs merge-base --is-ancestor "$YLT_PIN" HEAD 2>/dev/null; then
        ok "yalantinglibs 含 $YLT_PIN"
    else
        warn "yalantinglibs 不含 $YLT_PIN（检查 submodule 状态）"
    fi
fi

# ===================== 930 三补丁（幂等，只改工作区） =====================
if [ "$PATCH_930" = "true" ]; then
    log "应用 930 三补丁（round11 实测修复）"
    python3 - <<'PYEOF'
import pathlib

PATCHES = [
    ("mooncake-store/src/client_service.cpp",
     '<< " replicas=" << start_result.value().size()',
     '<< " replicas=" << start_result.value().replicas.size()'),
    ("mooncake-store/src/client_service.cpp",
     "DetermineFinalizeDecision(config, transfer_summary);",
     "DetermineFinalizeDecision(client_cfg, transfer_summary);"),
    ("mooncake-store/src/master_service.cpp",
     '        }  // allocator_access 在此释放；远程转发在锁外执行，避免分布式死锁\n'
     '\n'
     '        if (!allocation_result.has_value()) {',
     '        }  // allocator_access 在此释放；远程转发在锁外执行，避免分布式死锁\n'
     '\n'
     '        if (config.replica_num == 0) {\n'
     '            // 【930 pure-NoF patch】mem=0：跳过内存失败处理，直接进入下方 NoF 分配块\n'
     '        } else if (!allocation_result.has_value()) {'),
]

for rel, old, new in PATCHES:
    p = pathlib.Path(rel)
    text = p.read_text()
    if new in text:
        print(f"[SKIP] {rel}: already patched")
    elif old in text:
        p.write_text(text.replace(old, new, 1))
        print(f"[OK]   {rel}: patched")
    else:
        # 不中断：分支若已回灌，anchor 本来就不存在
        print(f"[WARN] {rel}: anchor not found（分支已变更或已回灌？人工核对）")
PYEOF
    git diff --stat || true
else
    warn "PATCH_930=false，跳过补丁（分支须已回灌，否则纯 NoF 会 -200）"
fi

# ===================== SPDK（可选编译） =====================
if [ "$SPDK_OK" != 1 ] && [ "$INSTALL_SPDK" = "true" ]; then
    log "编译安装 SPDK ${SPDK_COMMIT:0:9}"
    if [ ! -d "$SPDK_SRC_DIR/.git" ]; then
        mkdir -p "$(dirname "$SPDK_SRC_DIR")"
        git clone "$SPDK_REPO" "$SPDK_SRC_DIR"
    fi
    git -C "$SPDK_SRC_DIR" fetch --all --tags || true
    git -C "$SPDK_SRC_DIR" checkout "$SPDK_COMMIT"
    git -C "$SPDK_SRC_DIR" submodule update --init
    (
        cd "$SPDK_SRC_DIR"
        # e12b7cb21+ 必须 --disable-unit-tests，否则 spdk_get_ticks 链接回归
        ./configure --with-rdma --with-urma --disable-unit-tests
        make -j"$JOBS"
        make install || warn "make install 非零退出（通常是 spdk/python hatchling 步骤），以产物为准"
    )
    [ -f /usr/local/lib/libspdk_nvme.a ] \
        || die "SPDK 编译后仍缺 /usr/local/lib/libspdk_nvme.a"
    ok "SPDK 安装完成"
elif [ "$SPDK_OK" != 1 ]; then
    warn "SPDK 自检未通过但未指定 --install-spdk：若 /usr/local 已装好可忽略"
fi

# ===================== venv =====================
log "Python venv"
if [ ! -x "$VENV/bin/python" ]; then
    echo ">> 创建 venv: $VENV"
    mkdir -p "$(dirname "$VENV")"
    python3 -m venv "$VENV"
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"
python -m pip install -U pip >/dev/null 2>&1 || warn "pip 升级失败（可忽略）"
python -m pip install -q aiohttp paramiko >/dev/null 2>&1 \
    && ok "pip deps: aiohttp paramiko" || warn "aiohttp/paramiko 安装失败（运行期再装）"
VENV_PIP="$VENV/bin/pip"

# ===================== cmake + build + install =====================
[ -n "$BUILD_DIR" ] || BUILD_DIR="$SRC_ROOT/build"
if [ "$CLEAN_BUILD" = "true" ] && [ -d "$BUILD_DIR" ]; then
    echo ">> 清理构建目录 $BUILD_DIR"; rm -rf "$BUILD_DIR"
fi

# 锁放在构建目录里：源码树可能是别人只读的共享副本，不该往里写
mkdir -p "$BUILD_DIR"
exec 9>"$BUILD_DIR/.build.lock"
flock -n 9 || die "已有编译在运行（$BUILD_DIR/.build.lock）"

SPDIAG_ARGS=()
if [ -d /usr/lib64/cmake/SpDiag ]; then
    SPDIAG_ARGS=(-DMOONCAKE_ENABLE_SPDIAG=ON -DSpDiag_DIR=/usr/lib64/cmake/SpDiag
                 -DMOONCAKE_SPDIAG_SYSTEM_CLI=/usr/bin/spdiag)
else
    echo "  SpDiag 未安装，MOONCAKE_ENABLE_SPDIAG=OFF"
fi

export CXXFLAGS="${CXXFLAGS:-} -w"
export CFLAGS="${CFLAGS:-} -w"

log "cmake 配置"
CMAKE_ARGS=(
  -DUSE_UB=ON
  -DUSE_NOF=ON
  -DUSE_NOF_URMA=ON
  -DUSE_NOF_GPU_DIRECT=OFF
  -DUSE_CUDA=OFF
  -DUSE_URING=ON
  -DUSE_HTTP=ON
  -DUSE_ETCD="$USE_ETCD"
  -DSTORE_USE_ETCD="$USE_ETCD"
  -DWITH_TE=ON
  -DWITH_STORE=ON
  -DWITH_STORE_RUST=OFF
  -DWITH_P2P_STORE=OFF
  -DWITH_STORE_GO=OFF
  -DBUILD_BENCHMARK=ON
  -DBUILD_UNIT_TESTS=OFF
  -DBUILD_EXAMPLES=ON
  -DBUILD_SHARED_LIBS=OFF
  -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
  -DCMAKE_DISABLE_FIND_PACKAGE_AWSSDK=ON
  -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX"
  -DURMA_ROOT="$URMA_ROOT"
  -DURMA_LIBRARY="$UMDK_LIB"
  -DYLT_ENABLE_URMA=ON
  # yalantinglibs 的 YLT_ENABLE_URMA 会传播裸 "-lurma"，链接器默认先命中
  # /usr/lib64/liburma.so（旧版，缺 urma_register_seg_dmabuf）→ undefined reference。
  # 把 UMDK 的 lib 目录前置到链接搜索路径，让 -lurma 命中正确的那份。
  -DCMAKE_EXE_LINKER_FLAGS="-L$(dirname "$UMDK_LIB")"
  -DCMAKE_SHARED_LINKER_FLAGS="-L$(dirname "$UMDK_LIB")"
  -DCMAKE_MODULE_LINKER_FLAGS="-L$(dirname "$UMDK_LIB")"
)
[ -n "$UMDK_ROOT" ] && CMAKE_ARGS+=(-DUMDK_ROOT="$UMDK_ROOT")
[ "${#SPDIAG_ARGS[@]}" -gt 0 ] && CMAKE_ARGS+=("${SPDIAG_ARGS[@]}")
echo "  cmake -S $SRC_ROOT -B $BUILD_DIR ${CMAKE_ARGS[*]}"
cmake -S "$SRC_ROOT" -B "$BUILD_DIR" "${CMAKE_ARGS[@]}"

# store.so 由 cmake install 装进「configure 时探测到的 python」。若复用旧 build 目录且
# 换了 --venv，缓存里的解释器不会更新，install 会装到旧 venv —— 这里纠正。
CACHED_PY=$(grep -m1 '^_Python3_EXECUTABLE:INTERNAL=' "$BUILD_DIR/CMakeCache.txt" 2>/dev/null | cut -d= -f2-)
if [ -n "$CACHED_PY" ] && [ "$CACHED_PY" != "$VENV/bin/python3.11" ] && [ "$CACHED_PY" != "$VENV/bin/python" ]; then
    warn "cmake 缓存的 python 是 $CACHED_PY，与 --venv ($VENV) 不一致，重新配置"
    rm -f "$BUILD_DIR/CMakeCache.txt"
    cmake -S "$SRC_ROOT" -B "$BUILD_DIR" "${CMAKE_ARGS[@]}"
fi

log "编译（-j$JOBS）"
export GOPROXY="${GOPROXY:-http://mirrors.aliyun.com/goproxy,direct}"
export GOINSECURE="go.etcd.io/etcd"
export GOSUMDB="sum.golang.org"
cmake --build "$BUILD_DIR" -j"$JOBS" --target \
    stress_cluster_bench mooncake_master mooncake_client nof_register nof_unregister \
    engine store transfer_engine_bench

log "安装到 $INSTALL_PREFIX（含 venv site-packages）"
if [ ! -w "$INSTALL_PREFIX" ] && [ "$(id -u)" != 0 ]; then
    sudo -E cmake --build "$BUILD_DIR" --target install -j"$JOBS"
else
    cmake --build "$BUILD_DIR" --target install -j"$JOBS"
fi

# ===================== wheel（可选） =====================
WHEEL_DIR="$SRC_ROOT/mooncake-wheel/dist"
if [ "$WITH_WHEEL" = "true" ]; then
    log "构建 wheel"
    ( source "$VENV/bin/activate"; bash scripts/build_wheel.sh )
    ls "$WHEEL_DIR"/mooncake_transfer_engine-*.whl >/dev/null 2>&1 \
        || die "wheel 未产出（$WHEEL_DIR）"
    "$VENV_PIP" uninstall -y mooncake-transfer-engine >/dev/null 2>&1 || true
    "$VENV_PIP" install --force-reinstall "$WHEEL_DIR"/mooncake_transfer_engine-*.whl
    ok "wheel 已装入 $VENV"
fi

# ===================== 验收 =====================
log "验收"
MISS=0
for b in mooncake-store/src/mooncake_master mooncake-store/src/mooncake_client \
         mooncake-store/src/nof_register mooncake-store/benchmarks/stress_cluster_bench; do
    if [ -x "$BUILD_DIR/$b" ]; then ok "$b"; else warn "缺失 $b"; MISS=1; fi
done
for b in mooncake_master mooncake_client; do
    [ -x "$INSTALL_PREFIX/bin/$b" ] && ok "$INSTALL_PREFIX/bin/$b" \
        || { warn "缺失 $INSTALL_PREFIX/bin/$b"; MISS=1; }
done
python -c "from mooncake.store import MooncakeDistributedNoFRegister; print('  store.so OK')" \
    || { warn "store.so 导入失败"; MISS=1; }
[ "$MISS" = 0 ] || die "验收未通过（见上）"

# ===================== 分发 =====================
if [ "${#NODES[@]}" -eq 0 ]; then
    log "完成（未指定 --nodes，仅本机）"
    cat <<EOF

安装位置：
  bin  -> $INSTALL_PREFIX/bin
  lib  -> $INSTALL_PREFIX/lib
  python -> $VENV/lib*/python3.11/site-packages/mooncake

运行期环境变量：
  export PATH=$INSTALL_PREFIX/bin:\$PATH
  export LD_LIBRARY_PATH=$(dirname "$UMDK_LIB"):$INSTALL_PREFIX/lib:/usr/local/lib64:/usr/local/lib
  export SPDK_URMA_DEV_NAME=udmac0d1e2 SPDK_URMA_CAPSULE_TRANSPORT=urma
  export MC_HIFREQ_LOG_SAMPLE_RATE=0 MC_SLICE_SIZE=1048576
  export MC_STORE_MEMCPY=1 MC_STORE_MEMCPY_INLINE=1
EOF
    exit 0
fi

log "分发到: ${NODES[*]}"
for node in "${NODES[@]}"; do
    echo ">> $node"
    # 远端 $HOME 必须由远端解析（scp 走 SFTP，不展开变量）
    RHOME=$(ssh "$node" 'echo $HOME') || die "ssh $node 失败（需免密）"
    ssh "$node" "mkdir -p '$INSTALL_PREFIX/bin' '$INSTALL_PREFIX/lib' '$RHOME/mooncake-930-dist'" \
        || die "无法在 $node 创建目录（$INSTALL_PREFIX 需可写；否则用 --prefix 指到可写目录）"

    for b in "${BIN_TARGETS[@]}"; do
        src="$BUILD_DIR/mooncake-store/src/$b"
        [ -f "$src" ] || src="$BUILD_DIR/mooncake-store/benchmarks/$b"
        [ -f "$src" ] || { warn "$node: 跳过缺失的 $b"; continue; }
        scp -q "$src" "$node:$INSTALL_PREFIX/bin/$b.new" || die "scp $b -> $node 失败"
        ssh "$node" "chmod +x '$INSTALL_PREFIX/bin/$b.new' && mv -f '$INSTALL_PREFIX/bin/$b.new' '$INSTALL_PREFIX/bin/$b'"
    done

    for so in "${SO_TARGETS[@]}"; do
        src="$BUILD_DIR/mooncake-common/$so"
        [ "$so" = "libetcd_wrapper.so" ] && src="$BUILD_DIR/mooncake-common/etcd/$so"
        [ -f "$src" ] || continue
        scp -q "$src" "$node:$INSTALL_PREFIX/lib/$so.new" \
            && ssh "$node" "mv -f '$INSTALL_PREFIX/lib/$so.new' '$INSTALL_PREFIX/lib/$so'"
    done

    scp -q "$WHEEL_DIR"/mooncake_transfer_engine-*.whl "$node:$RHOME/mooncake-930-dist/" \
        || die "scp wheel 到 $node 失败"
    ssh "$node" "
set -e
V='$REMOTE_VENV'
[ -x \"\$V/bin/python\" ] || python3 -m venv \"\$V\"
\"\$V/bin/pip\" install -q --force-reinstall '$RHOME'/mooncake-930-dist/mooncake_transfer_engine-*.whl
echo \"  [\$(hostname)] wheel OK -> \$V\"
"
done
ok "分发完成"
