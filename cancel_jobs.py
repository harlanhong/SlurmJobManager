#!/usr/bin/env python3

import argparse
import subprocess
import re
from typing import List, Optional, Dict, Set
from job_manager import JobManager
import json
import os

def get_running_jobs(user: Optional[str] = None) -> List[str]:
    """获取正在运行的Slurm作业ID列表"""
    try:
        cmd = ["squeue"]
        if user:
            cmd.extend(["-u", user])
        cmd.extend(["-h", "-o", "%i"])  # 只输出作业ID
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return [job_id for job_id in result.stdout.strip().split('\n') if job_id]
    except subprocess.CalledProcessError as e:
        print(f"获取作业列表失败: {e}")
        return []

def match_job_pattern(pattern: str, job_ids: List[str]) -> Set[str]:
    """匹配作业ID模式，支持通配符匹配"""
    # 将通配符模式转换为正则表达式
    if '*' in pattern:
        # 转义所有特殊字符，但保留 *
        regex_pattern = re.escape(pattern).replace('\\*', '.*')
    else:
        regex_pattern = re.escape(pattern)
    
    # 编译正则表达式
    regex = re.compile(f"^{regex_pattern}$")
    
    # 返回所有匹配的作业ID
    return {job_id for job_id in job_ids if regex.match(job_id)}

def get_jobs_by_pattern(patterns: List[str], manager: JobManager) -> Set[str]:
    """根据模式获取匹配的作业ID"""
    matched_jobs = set()
    all_jobs = list(manager.active_jobs.keys())
    
    for pattern in patterns:
        # 对每个模式进行匹配
        matched = match_job_pattern(pattern, all_jobs)
        matched_jobs.update(matched)
    
    return matched_jobs

def cancel_jobs_by_job_id(job_patterns: List[str]) -> bool:
    """通过job_id模式取消作业"""
    if not job_patterns:
        print("没有需要取消的作业")
        return True

    # 创建JobManager实例
    manager = JobManager(
        max_concurrent_jobs=1,
        check_interval=1.0,
        verbose=True
    )
    
    # 获取匹配的作业ID
    matched_jobs = get_jobs_by_pattern(job_patterns, manager)
    
    if not matched_jobs:
        print(f"没有找到匹配的作业")
        return True
    
    print(f"找到以下匹配的作业: {', '.join(sorted(matched_jobs))}")
    
    success = True
    cancelled_jobs = []
    failed_jobs = []
    
    for job_id in matched_jobs:
        if job_id in manager.active_jobs:
            print(f"正在取消作业 {job_id}...")
            if manager.active_jobs[job_id].cancel():
                cancelled_jobs.append(job_id)
            else:
                failed_jobs.append(job_id)
                success = False
    
    if cancelled_jobs:
        print(f"成功取消以下作业: {', '.join(cancelled_jobs)}")
    if failed_jobs:
        print(f"以下作业取消失败: {', '.join(failed_jobs)}")
    
    return success

def cancel_slurm_jobs(job_ids: List[str]) -> bool:
    """通过Slurm ID取消作业"""
    if not job_ids:
        print("没有需要取消的作业")
        return True
        
    try:
        cmd = ["scancel"] + job_ids
        subprocess.run(cmd, check=True)
        print(f"成功取消以下Slurm作业: {', '.join(job_ids)}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"取消作业失败: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="取消正在运行的作业")
    parser.add_argument("-u", "--user", help="指定用户名（默认为当前用户）")
    parser.add_argument("-j", "--job-ids", nargs="+", 
                      help="指定要取消的job_id列表或模式（支持通配符*，例如：python_task_*）")
    parser.add_argument("-s", "--slurm-ids", nargs="+", help="指定要取消的Slurm作业ID列表")
    args = parser.parse_args()

    if args.job_ids:
        # 如果指定了job_id或模式，通过job_id取消作业
        cancel_jobs_by_job_id(args.job_ids)
    elif args.slurm_ids:
        # 如果指定了Slurm ID，直接通过Slurm ID取消作业
        cancel_slurm_jobs(args.slurm_ids)
    else:
        # 否则取消所有正在运行的作业
        running_jobs = get_running_jobs(args.user)
        if running_jobs:
            print(f"发现以下正在运行的作业: {', '.join(running_jobs)}")
            cancel_slurm_jobs(running_jobs)
        else:
            print("没有找到正在运行的作业")

if __name__ == "__main__":
    main()