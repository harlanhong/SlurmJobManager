from typing import List, Dict, Any, Optional
from collections import deque
import time
from datetime import datetime, timedelta
from .job import Job, JobStatus

class JobManager:
    def __init__(
        self,
        max_concurrent_jobs: int = 4,
        check_interval: float = 60.0,
        max_retries: int = 3,
        print_interval: float = 300.0,  # 默认每5分钟打印一次状态
        verbose: bool = True
    ):
        """
        初始化任务管理器
        
        Args:
            max_concurrent_jobs: 最大并发任务数
            check_interval: 检查任务状态的时间间隔（秒）
            max_retries: 任务失败后最大重试次数
            print_interval: 打印状态信息的时间间隔（秒）
            verbose: 是否打印详细信息
        """
        self.max_concurrent_jobs = max_concurrent_jobs
        self.check_interval = check_interval
        self.max_retries = max_retries
        self.print_interval = print_interval
        self.verbose = verbose
        self.last_print_time = 0
        
        self.active_jobs: Dict[str, Job] = {}  # 正在运行的任务
        self.pending_jobs: deque[Job] = deque()  # 等待执行的任务
        self.completed_jobs: Dict[str, Job] = {}  # 已完成的任务
        self.failed_jobs: Dict[str, Job] = {}  # 失败的任务
        
        self.retry_counts: Dict[str, int] = {}  # 任务重试次数记录

    def add_job(
        self,
        job_id: str,
        script_path: str,
        args: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> bool:
        """
        添加新任务到管理器
        
        Args:
            job_id: 任务唯一标识符
            script_path: 要执行的脚本路径
            args: 脚本的参数字典
            **kwargs: 传递给Job构造函数的其他参数
        """
        if job_id in self.active_jobs or job_id in self.completed_jobs:
            print(f"任务 {job_id} 已存在")
            return False
            
        job = Job(job_id, script_path, args, **kwargs)
        
        if len(self.active_jobs) < self.max_concurrent_jobs:
            return self._submit_job(job)
        else:
            self.pending_jobs.append(job)
            print(f"任务 {job_id} 已添加到等待队列")
            return True

    def _submit_job(self, job: Job) -> bool:
        """
        提交任务到Slurm
        """
        if job.submit():
            self.active_jobs[job.job_id] = job
            print(f"任务 {job.job_id} 已提交到Slurm (ID: {job.slurm_id})")
            return True
        return False

    def _handle_completed_job(self, job: Job):
        """
        处理已完成的任务
        """
        job_id = job.job_id
        self.completed_jobs[job_id] = job
        del self.active_jobs[job_id]
        if job_id in self.retry_counts:
            del self.retry_counts[job_id]
        print(f"任务 {job_id} 已完成，运行时间: {job.get_runtime():.2f}秒")

    def _handle_failed_job(self, job: Job):
        """
        处理失败的任务
        """
        job_id = job.job_id
        retry_count = self.retry_counts.get(job_id, 0)
        
        if retry_count < self.max_retries:
            self.retry_counts[job_id] = retry_count + 1
            print(f"任务 {job_id} 失败，正在重试 ({retry_count + 1}/{self.max_retries})")
            self.pending_jobs.appendleft(job)  # 优先重试失败的任务
        else:
            print(f"任务 {job_id} 失败且超过最大重试次数")
            self.failed_jobs[job_id] = job
            if job_id in self.retry_counts:
                del self.retry_counts[job_id]
        
        del self.active_jobs[job_id]

    def update_status(self):
        """
        更新所有活动任务的状态，并管理任务队列
        """
        current_time = time.time()
        
        # 检查活动任务的状态
        for job in list(self.active_jobs.values()):
            status = job.check_status()
            
            if status == JobStatus.COMPLETED:
                self._handle_completed_job(job)
            elif status == JobStatus.FAILED:
                self._handle_failed_job(job)
        
        # 提交等待队列中的任务
        while len(self.active_jobs) < self.max_concurrent_jobs and self.pending_jobs:
            next_job = self.pending_jobs.popleft()
            if not self._submit_job(next_job):
                self.failed_jobs[next_job.job_id] = next_job
        
        # 定期打印状态信息
        if self.verbose and (current_time - self.last_print_time >= self.print_interval):
            self._print_status()
            self.last_print_time = current_time

    def run(self):
        """
        运行任务管理器主循环
        """
        try:
            while self.active_jobs or self.pending_jobs:
                self.update_status()
                time.sleep(self.check_interval)
        except KeyboardInterrupt:
            print("\n正在取消所有活动任务...")
            self.cancel_all_jobs()

    def cancel_all_jobs(self):
        """
        取消所有活动任务
        """
        for job in self.active_jobs.values():
            job.cancel()
        self.pending_jobs.clear()

    def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """
        获取指定任务的状态
        """
        if job_id in self.active_jobs:
            return self.active_jobs[job_id].status
        elif job_id in self.completed_jobs:
            return self.completed_jobs[job_id].status
        elif job_id in self.failed_jobs:
            return self.failed_jobs[job_id].status
        return None

    def get_all_jobs_status(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有任务的状态信息
        """
        status_info = {}
        
        for jobs in [self.active_jobs, self.completed_jobs, self.failed_jobs]:
            for job_id, job in jobs.items():
                runtime = job.get_runtime()
                runtime_str = str(timedelta(seconds=int(runtime))) if runtime else "N/A"
                
                status_info[job_id] = {
                    "status": job.status.value,
                    "slurm_id": job.slurm_id,
                    "runtime": runtime_str,
                    "retry_count": self.retry_counts.get(job_id, 0),
                    "partition": job.partition,
                    "num_gpus": job.num_gpus,
                    "memory": job.memory
                }
        
        for job in self.pending_jobs:
            status_info[job.job_id] = {
                "status": "QUEUED",
                "slurm_id": None,
                "runtime": "N/A",
                "retry_count": self.retry_counts.get(job.job_id, 0),
                "partition": job.partition,
                "num_gpus": job.num_gpus,
                "memory": job.memory
            }
            
        return status_info
        
    def _print_status(self):
        """打印当前任务状态信息"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n=== 任务状态更新 ({current_time}) ===")
        
        # 获取所有任务状态
        all_status = self.get_all_jobs_status()
        
        # 按状态分类任务
        status_groups = {
            "RUNNING": [],
            "QUEUED": [],
            "COMPLETED": [],
            "FAILED": [],
            "CANCELLED": []
        }
        
        for job_id, info in all_status.items():
            status = info["status"]
            status_groups.get(status, []).append((job_id, info))
        
        # 打印活动任务
        if status_groups["RUNNING"]:
            print("\n活动任务:")
            for job_id, info in status_groups["RUNNING"]:
                print(f"  - {job_id} (Slurm ID: {info['slurm_id']}):")
                print(f"    状态: {info['status']}")
                print(f"    运行时间: {info['runtime']}")
                print(f"    资源: {info['partition']}, {info['num_gpus']}GPU, {info['memory']}内存")
                if info['retry_count'] > 0:
                    print(f"    重试次数: {info['retry_count']}")
        
        # 打印等待任务
        if status_groups["QUEUED"]:
            print("\n等待任务:")
            for job_id, info in status_groups["QUEUED"]:
                print(f"  - {job_id} (将使用: {info['partition']}, {info['num_gpus']}GPU, {info['memory']}内存)")
        
        # 打印最近完成的任务
        if status_groups["COMPLETED"]:
            print("\n已完成任务:")
            for job_id, info in status_groups["COMPLETED"]:
                print(f"  - {job_id}: 运行时间 {info['runtime']}")
        
        # 打印失败任务
        if status_groups["FAILED"] or status_groups["CANCELLED"]:
            print("\n失败/取消的任务:")
            for status in ["FAILED", "CANCELLED"]:
                for job_id, info in status_groups[status]:
                    print(f"  - {job_id} ({status})")
                    if info['retry_count'] > 0:
                        print(f"    重试次数: {info['retry_count']}")
        
        print("\n任务统计:")
        print(f"  运行中: {len(status_groups['RUNNING'])}")
        print(f"  等待中: {len(status_groups['QUEUED'])}")
        print(f"  已完成: {len(status_groups['COMPLETED'])}")
        print(f"  已失败: {len(status_groups['FAILED'])}")
        print(f"  已取消: {len(status_groups['CANCELLED'])}")
        print("="*50)
