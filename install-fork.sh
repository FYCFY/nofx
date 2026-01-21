#!/bin/bash
#
# NOFX Fork One-Click Installation/Update Script
# Source: https://github.com/FYCFY/nofx
#
# 用法（默认安装到 ~/nofx）:
#   curl -fsSL https://raw.githubusercontent.com/FYCFY/nofx/dev/install-fork.sh | bash
# 自定义目录:
#   curl -fsSL https://raw.githubusercontent.com/FYCFY/nofx/dev/install-fork.sh | bash -s -- /opt/nofx
#
# 镜像可通过环境变量覆盖：
#   NOFX_BACKEND_IMAGE=my-registry/nofx-backend:dev
#   NOFX_FRONTEND_IMAGE=my-registry/nofx-frontend:dev

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

INSTALL_DIR="${1:-$HOME/nofx}"
COMPOSE_FILE="docker-compose.fork.yml"
GITHUB_RAW="${GITHUB_RAW:-https://raw.githubusercontent.com/FYCFY/nofx/dev}"

echo -e "${BLUE}"
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                NOFX Fork - One Click Deploy                ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

check_docker() {
    echo -e "${YELLOW}Checking Docker & Compose...${NC}"
    if ! command -v docker >/dev/null 2>&1; then
        echo -e "${RED}Docker 未安装，请先安装 Docker.${NC}"
        exit 1
    fi
    if ! docker info >/dev/null 2>&1; then
        echo -e "${RED}Docker 守护进程未运行，请启动后重试。${NC}"
        exit 1
    fi
    if docker compose version >/dev/null 2>&1; then
        COMPOSE_CMD="docker compose"
    elif command -v docker-compose >/dev/null 2>&1; then
        COMPOSE_CMD="docker-compose"
    else
        echo -e "${RED}未检测到 Docker Compose，请安装后重试。${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Docker 就绪${NC}"
}

setup_directory() {
    echo -e "${YELLOW}准备安装目录: ${INSTALL_DIR}${NC}"
    mkdir -p "$INSTALL_DIR"
    cd "$INSTALL_DIR"
    echo -e "${GREEN}✓ 目录已就绪${NC}"
}

download_files() {
    echo -e "${YELLOW}下载 Compose 配置...${NC}"
    curl -fsSL "$GITHUB_RAW/$COMPOSE_FILE" -o docker-compose.yml
    echo -e "${GREEN}✓ 配置下载完成${NC}"
}

generate_env() {
    echo -e "${YELLOW}检查/生成 .env...${NC}"
    if [ -f ".env" ]; then
        echo -e "${GREEN}✓ 已存在 .env，跳过生成${NC}"
        return
    fi
    JWT_SECRET=$(openssl rand -base64 32)
    DATA_ENCRYPTION_KEY=$(openssl rand -base64 32)
    RSA_PRIVATE_KEY=$(openssl genrsa 2048 2>/dev/null | tr '\n' '\\' | sed 's/\\/\\n/g' | sed 's/\\n$//')
    cat > .env << EOF
# NOFX Fork auto-generated config
NOFX_BACKEND_PORT=8080
NOFX_FRONTEND_PORT=3000
TZ=Asia/Shanghai
JWT_SECRET=${JWT_SECRET}
DATA_ENCRYPTION_KEY=${DATA_ENCRYPTION_KEY}
RSA_PRIVATE_KEY=${RSA_PRIVATE_KEY}
# 覆盖镜像示例:
# NOFX_BACKEND_IMAGE=my-registry/nofx-backend:dev
# NOFX_FRONTEND_IMAGE=my-registry/nofx-frontend:dev
EOF
    echo -e "${GREEN}✓ 已生成 .env${NC}"
}

pull_images() {
    echo -e "${YELLOW}拉取镜像...${NC}"
    $COMPOSE_CMD pull
    echo -e "${GREEN}✓ 镜像已拉取${NC}"
}

start_services() {
    echo -e "${YELLOW}启动服务...${NC}"
    $COMPOSE_CMD up -d
    echo -e "${GREEN}✓ 服务已启动${NC}"
}

wait_for_services() {
    echo -e "${YELLOW}等待后端健康检查 (最多60秒)...${NC}"
    for _ in {1..30}; do
        if curl -fsS http://127.0.0.1:${NOFX_BACKEND_PORT:-8080}/api/health >/dev/null 2>&1; then
            echo -e "${GREEN}✓ 后端已就绪${NC}"
            return
        fi
        sleep 2
    done
    echo -e "${YELLOW}后端健康检查未通过，请手动查看 docker 日志。${NC}"
}

summary() {
    echo ""
    echo -e "${GREEN}==============================================================${NC}"
    echo -e "${GREEN}部署完成！${NC}"
    echo -e "后端: http://127.0.0.1:${NOFX_BACKEND_PORT:-8080}"
    echo -e "前端: http://127.0.0.1:${NOFX_FRONTEND_PORT:-3000}"
    echo -e "自定义镜像: 在 .env 设置 NOFX_BACKEND_IMAGE / NOFX_FRONTEND_IMAGE 后再运行本脚本即可更新"
    echo -e "${GREEN}==============================================================${NC}"
}

main() {
    check_docker
    setup_directory
    download_files
    generate_env
    pull_images
    start_services
    wait_for_services
    summary
}

main "$@"
