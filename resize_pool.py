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

def resize_pool(new_size: int):
    """调整任务池大小"""
    # 查找任务管理器进程
    manager_pid = find_manager_pid()
    if not manager_pid:
        print("错误：未找到运行中的任务管理器进程")
        return False
    
    try:
        # 将新的大小写入临时文件
        with open("/tmp/slurm_pool_size", "w") as f:
            f.write(str(new_size))
        
        # 发送信号给管理器进程
        os.kill(manager_pid, signal.SIGUSR1)
        print(f"已发送调整请求：池大小 -> {new_size}")
        return True
    except Exception as e:
        print(f"调整失败: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("用法: python resize_pool.py <new_size>")
        print("示例: python resize_pool.py 1")
        sys.exit(1)
    
    try:
        new_size = int(sys.argv[1])
        if new_size <= 0:
            print("错误：池大小必须大于0")
            sys.exit(1)
        
        if resize_pool(new_size):
            print("调整命令已发送，请查看任务管理器日志获取结果")
        else:
            print("调整失败")
    except ValueError:
        print("错误：请输入有效的数字")
        sys.exit(1)
