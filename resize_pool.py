#!/usr/bin/env python3
import os
import signal
import sys
import psutil

def find_manager_pid():
    """从PID文件中读取任务管理器进程ID"""
    try:
        with open("/tmp/slurm_job_manager.pid", "r") as f:
            pid = int(f.read().strip())
            # 验证进程是否存在
            try:
                os.kill(pid, 0)  # 发送信号0来检查进程是否存在
                return pid
            except OSError:
                return None
    except (FileNotFoundError, ValueError):
        return None

def cancel_resize():
    """取消resize pool功能"""
    manager_pid = find_manager_pid()
    if not manager_pid:
        print("错误：未找到运行中的任务管理器进程")
        return False
    
    try:
        # 删除临时文件
        if os.path.exists("/tmp/slurm_pool_size"):
            os.remove("/tmp/slurm_pool_size")
        
        print("已取消resize pool功能")
        return True
    except Exception as e:
        print(f"取消失败: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] != "cancel":
        print("用法: python resize_pool.py cancel")
        print("示例: python resize_pool.py cancel")
        sys.exit(1)
    
    if cancel_resize():
        print("取消命令已执行")
    else:
        print("取消失败")
