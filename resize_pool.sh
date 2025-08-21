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

# 查找任务管理器进程
MANAGER_PID=$(ps aux | grep "python" | grep "job_manager.py" | grep -v "grep" | awk '{print $2}')

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
