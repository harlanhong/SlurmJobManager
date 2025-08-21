#!/bin/bash

# 检查参数
if [ $# -ne 1 ]; then
    echo "用法: ./resize_pool.sh <new_size>"
    echo "示例: ./resize_pool.sh 1"
    exit 1
fi

# 检查输入是否为数字
if ! [[ $1 =~ ^[0-9]+$ ]]; then
    echo "错误：请输入有效的数字"
    exit 1
fi

# 检查输入是否大于0
if [ $1 -le 0 ]; then
    echo "错误：池大小必须大于0"
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

if [ -z "$MANAGER_PID" ]; then
    echo "错误：未找到运行中的任务管理器进程"
    exit 1
fi

# 写入新的池大小
echo $1 > /tmp/slurm_pool_size

# 发送信号
kill -SIGUSR1 $MANAGER_PID

echo "已发送调整请求：池大小 -> $1"
echo "请查看任务管理器日志获取结果"
