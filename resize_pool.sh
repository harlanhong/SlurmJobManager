#!/bin/bash

# 检查参数
if [ $# -ne 1 ] || [ "$1" != "cancel" ]; then
    echo "用法: ./resize_pool.sh cancel"
    echo "示例: ./resize_pool.sh cancel"
    exit 1
fi

# 从PID文件读取进程ID
if [ ! -f "/tmp/slurm_job_manager.pid" ]; then
    echo "错误：未找到任务管理器PID文件"
    exit 1
fi

MANAGER_PID=$(cat /tmp/slurm_job_manager.pid)

# 验证进程是否存在
if ! kill -0 $MANAGER_PID 2>/dev/null; then
    echo "错误：任务管理器进程不存在"
    rm -f /tmp/slurm_job_manager.pid
    exit 1
fi

# 删除临时文件
if [ -f "/tmp/slurm_pool_size" ]; then
    rm -f /tmp/slurm_pool_size
fi

echo "已取消resize pool功能"
