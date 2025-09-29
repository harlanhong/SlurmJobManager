#!/usr/bin/env python3

import argparse
import subprocess
import re
from typing import List, Optional, Dict, Tuple
import json
import os

def get_running_jobs(user: Optional[str] = None) -> List[Tuple[str, str, str]]:
    """获取正在运行的作业信息列表，返回 (job_id, slurm_id, name) 元组列表"""
    try:
        cmd = ["squeue"]
        if user:
            cmd.extend(["-u", user])
        # 获取作业ID、名称和状态
        cmd.extend(["-h", "-o", "%i|%j|%t"])
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        jobs = []
        for line in result.stdout.strip().split('\n'):
            if line:
                slurm_id, name, status = line.split('|')
                if status in ['R', 'PD']:  # 只处理正在运行或等待的作业
                    jobs.append((name, slurm_id, status))
        return jobs
    except subprocess.CalledProcessError as e:
        print(f"获取作业列表失败: {e}")
        return []

def match_job_pattern(pattern: str, job_names: List[str]) -> List[str]:
    """匹配作业名称模式，支持通配符匹配"""
    # 将通配符模式转换为正则表达式
    if '*' in pattern:
        # 转义所有特殊字符，但保留 *
        regex_pattern = re.escape(pattern).replace('\\*', '.*')
    else:
        regex_pattern = re.escape(pattern)
    
    # 编译正则表达式
    regex = re.compile(f"^{regex_pattern}$")
    
    # 返回所有匹配的作业名称
    return [name for name in job_names if regex.match(name)]

def cancel_jobs_by_patterns(patterns: List[str], user: Optional[str] = None) -> bool:
    """通过作业名称模式取消作业"""
    if not patterns:
        print("没有指定要取消的作业模式")
        return True

    # 获取所有运行中的作业
    running_jobs = get_running_jobs(user)
    if not running_jobs:
        print("没有找到正在运行的作业")
        return True

    # 对每个模式进行匹配
    jobs_to_cancel = set()
    for pattern in patterns:
        job_names = [job[0] for job in running_jobs]  # 获取所有作业名称
        matched_names = match_job_pattern(pattern, job_names)
        
        # 找到匹配名称对应的Slurm ID
        for name in matched_names:
            for job_name, slurm_id, status in running_jobs:
                if job_name == name:
                    jobs_to_cancel.add((job_name, slurm_id, status))

    if not jobs_to_cancel:
        print(f"没有找到匹配的作业")
        return True

    # 显示找到的作业
    print("\n找到以下匹配的作业:")
    for job_name, slurm_id, status in sorted(jobs_to_cancel):
        status_str = "运行中" if status == 'R' else "等待中"
        print(f"  - {job_name} (Slurm ID: {slurm_id}, 状态: {status_str})")

    # 确认取消
    try:
        # 执行取消操作
        slurm_ids = [job[1] for job in jobs_to_cancel]
        cmd = ["scancel"] + slurm_ids
        subprocess.run(cmd, check=True)
        print(f"\n成功取消以下作业:")
        for job_name, slurm_id, _ in sorted(jobs_to_cancel):
            print(f"  - {job_name} (Slurm ID: {slurm_id})")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n取消作业失败: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="取消正在运行的作业")
    parser.add_argument("-u", "--user", help="指定用户名（默认为当前用户）")
    parser.add_argument("-j", "--job-patterns", nargs="+", 
                      help="指定要取消的作业名称模式（支持通配符*，例如：python_task_*）")
    args = parser.parse_args()

    if args.job_patterns:
        # 通过模式取消特定作业
        cancel_jobs_by_patterns(args.job_patterns, args.user)
    else:
        # 显示当前运行的作业
        running_jobs = get_running_jobs(args.user)
        if running_jobs:
            print("\n当前正在运行的作业:")
            for job_name, slurm_id, status in sorted(running_jobs):
                status_str = "运行中" if status == 'R' else "等待中"
                print(f"  - {job_name} (Slurm ID: {slurm_id}, 状态: {status_str})")
        else:
            print("没有找到正在运行的作业")

if __name__ == "__main__":
    main()