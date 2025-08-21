from enum import Enum
import subprocess
import time
import os
from typing import Optional, Dict, Any, List

class JobStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

class Job:
    def __init__(
        self,
        job_id: str,
        script_path: str,
        args: Optional[Dict[str, Any]] = None,
        partition: str = "default",
        num_gpus: int = 1,
        num_cpus: int = 1,
        memory: str = "16G",
        conda_env: Optional[str] = None,
        working_dir: Optional[str] = None,
        executor: str = "python",
        executor_args: Optional[List[str]] = None,
        time_limit: str = "24:00:00",
        mail_type: Optional[str] = None,
        mail_user: Optional[str] = None,
        log_dir: Optional[str] = None,
        extra_sbatch_params: Optional[Dict[str, Any]] = None,
        script_args_separator: str = "--",  # 用于分隔脚本参数的标记
    ):
        """
        初始化一个Slurm任务
        
        Args:
            job_id: 任务唯一标识符
            script_path: 要执行的脚本路径
            args: 脚本的参数字典
            partition: Slurm分区名称
            num_gpus: 需要的GPU数量
            num_cpus: 需要的CPU数量
            memory: 需要的内存大小
            conda_env: conda环境名称，如果需要在特定环境中运行
            working_dir: 运行脚本的工作目录
            executor: 执行器程序（例如："python"、"blender"等）
            executor_args: 执行器的额外参数
            time_limit: 任务时间限制 (格式: "HH:MM:SS" 或 "D-HH:MM:SS")
            mail_type: 邮件通知类型 (例如: "END,FAIL")
            mail_user: 通知邮件地址
            log_dir: 日志文件目录
            extra_sbatch_params: 额外的SBATCH参数字典
            script_args_separator: 用于分隔脚本参数的标记（例如：--）
        """
        self.job_id = job_id
        self.script_path = script_path
        self.args = args or {}
        self.partition = partition
        self.num_gpus = num_gpus
        self.num_cpus = num_cpus
        self.memory = memory
        self.conda_env = conda_env
        self.working_dir = working_dir
        self.executor = executor
        self.executor_args = executor_args or []
        self.time_limit = time_limit
        self.mail_type = mail_type
        self.mail_user = mail_user
        self.log_dir = log_dir
        self.extra_sbatch_params = extra_sbatch_params or {}
        self.script_args_separator = script_args_separator
        
        self.slurm_id: Optional[str] = None
        self.status = JobStatus.PENDING
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

    def _create_job_script(self) -> str:
        """创建包含环境设置的作业脚本"""
        script_content = "#!/bin/bash\n\n"
        
        # 添加SBATCH参数
        script_content += f"#SBATCH --job-name={self.job_id}         # Job name\n"
        
        # 设置日志输出
        if self.log_dir:
            log_path = f"{self.log_dir}/%x.log"
            script_content += f"#SBATCH --output={log_path}         # Standard output and error log\n"
        
        script_content += f"#SBATCH --partition={self.partition}           # Partition name\n"
        script_content += "#SBATCH --ntasks=1                     # Run a single task\n"
        script_content += f"#SBATCH --cpus-per-task={self.num_cpus}             # Number of CPU cores per task\n"
        script_content += f"#SBATCH --mem={self.memory}                       # Total memory per node\n"
        script_content += f"#SBATCH --time={self.time_limit}                # Time limit hrs:min:sec\n"
        
        if self.num_gpus > 0:
            script_content += f"#SBATCH --gres=gpu:{self.num_gpus}\n"
            
        if self.mail_type and self.mail_user:
            script_content += f"#SBATCH --mail-type={self.mail_type}           # Mail events\n"
            script_content += f"#SBATCH --mail-user={self.mail_user}  # Where to send mail\n"
        
        # 添加额外的SBATCH参数
        for key, value in self.extra_sbatch_params.items():
            script_content += f"#SBATCH --{key}={value}\n"
        
        script_content += "\n# Print some info for debugging\n"
        script_content += f"export SBATCH_PARTITION={self.partition}\n"
        script_content += 'echo "Running on host: $(hostname)"\n'
        script_content += 'echo "Time is: $(date)"\n'
        script_content += 'echo "Directory is: $(pwd)"\n\n'
        
        # 如果指定了工作目录，添加cd命令
        if self.working_dir:
            script_content += f"cd {self.working_dir}\n\n"
        
        # 如果指定了conda环境，添加conda激活命令
        if self.conda_env:
            script_content += "# Load any required modules\n"
            script_content += "source $(conda info --base)/etc/profile.d/conda.sh\n"
            script_content += f"conda activate {self.conda_env}\n\n"
        
        # 构建命令
        if "blender" in self.executor.lower():
            # Blender特殊处理
            cmd = [self.executor]  # 使用完整的blender路径
            
            # 添加--background参数（如果没有在executor_args中指定）
            if "--background" not in self.executor_args:
                cmd.append("--background")
            
            # 添加其他执行器参数
            cmd.extend(self.executor_args)
            
            # 添加blend文件（如果有）
            blend_file = None
            for key, value in list(self.args.items()):
                if key.startswith(("blend_file", "animation_blendfile")):
                    blend_file = value
                    del self.args[key]
                    break
            
            # 添加Python脚本
            cmd.extend(["--python", self.script_path])
            
            # 添加脚本参数分隔符
            cmd.append(self.script_args_separator)
            
            # 添加其他参数（每个参数独立一行）
            script_content += " \\\n".join([" ".join(cmd)] + [
                f"--{key}='{str(value)}' \\"
                for key, value in self.args.items()
            ]) + "\n"
        else:
            # 其他执行器的默认处理方式
            cmd = [self.executor] + self.executor_args + [self.script_path]
            if self.args:
                cmd.append(self.script_args_separator)
                cmd.extend([f"--{k}={v}" for k, v in self.args.items()])
            script_content += " ".join(cmd) + "\n"
        
        script_content += "# End of script\n"
        
        # 创建临时作业脚本
        script_path = f"/tmp/slurm_job_{self.job_id}.sh"
        with open(script_path, "w") as f:
            f.write(script_content)
        
        # 设置执行权限
        os.chmod(script_path, 0o755)
        return script_path

    def _build_sbatch_command(self, job_script_path: str) -> str:
        """构建sbatch命令"""
        cmd = [
            "sbatch",
            f"--partition={self.partition}",
            f"--gres=gpu:{self.num_gpus}",
            f"--cpus-per-task={self.num_cpus}",
            f"--mem={self.memory}",
            f"--job-name={self.job_id}",
            job_script_path
        ]
        return " ".join(cmd)

    def submit(self) -> bool:
        """提交任务到Slurm"""
        try:
            # 创建作业脚本
            job_script = self._create_job_script()
            
            # 构建并执行sbatch命令
            cmd = self._build_sbatch_command(job_script)
            result = subprocess.run(
                cmd,
                shell=True,
                check=True,
                capture_output=True,
                text=True
            )
            
            # 从sbatch输出中提取Slurm作业ID
            self.slurm_id = result.stdout.strip().split()[-1]
            self.status = JobStatus.RUNNING
            self.start_time = time.time()
            
            # 清理临时作业脚本
            try:
                os.remove(job_script)
            except OSError:
                pass  # 忽略清理错误
                
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"提交作业失败: {e}")
            self.status = JobStatus.FAILED
            return False

    def check_status(self) -> JobStatus:
        """检查任务状态"""
        if not self.slurm_id:
            return self.status

        try:
            cmd = f"squeue -j {self.slurm_id} -h"
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0 and result.stdout.strip():
                # 作业仍在运行
                self.status = JobStatus.RUNNING
            else:
                # 检查作业是否成功完成
                sacct_cmd = f"sacct -j {self.slurm_id} -o State -n"
                sacct_result = subprocess.run(
                    sacct_cmd,
                    shell=True,
                    capture_output=True,
                    text=True
                )
                
                state = sacct_result.stdout.strip().split()[0]
                if state == "COMPLETED":
                    self.status = JobStatus.COMPLETED
                    if not self.end_time:
                        self.end_time = time.time()
                elif state in ["FAILED", "TIMEOUT", "OUT_OF_MEMORY"]:
                    self.status = JobStatus.FAILED
                    if not self.end_time:
                        self.end_time = time.time()
                
        except subprocess.CalledProcessError as e:
            print(f"检查作业状态失败: {e}")
            
        return self.status

    def cancel(self) -> bool:
        """取消任务"""
        if not self.slurm_id:
            return True

        try:
            cmd = f"scancel {self.slurm_id}"
            subprocess.run(cmd, shell=True, check=True)
            self.status = JobStatus.CANCELLED
            self.end_time = time.time()
            return True
        except subprocess.CalledProcessError as e:
            print(f"取消作业失败: {e}")
            return False

    def get_runtime(self) -> Optional[float]:
        """获取任务运行时间（秒）"""
        if not self.start_time:
            return None
        end = self.end_time or time.time()
        return end - self.start_time