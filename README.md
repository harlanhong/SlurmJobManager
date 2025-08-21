# Slurm Job Manager

A Python-based job manager for Slurm that helps you manage and monitor multiple jobs with features like job queuing, automatic retries, and status monitoring.

## Features

- **Job Queue Management**
  - Limit concurrent running jobs
  - Automatic job submission when slots become available
  - Priority queue for failed job retries
  - Dynamic pool size adjustment

- **Resource Management**
  - GPU allocation
  - CPU cores configuration
  - Memory allocation
  - Partition selection
  - Runtime resource reallocation

- **Environment Support**
  - Conda environment activation
  - Working directory configuration
  - Custom executor support (Python, Blender, etc.)

- **Status Monitoring**
  - Real-time job status updates
  - Periodic status printing
  - Detailed runtime statistics
  - Email notifications for job completion/failure
  - Status logging to file
  - Background running mode

- **Error Handling**
  - Automatic job retries on failure
  - Configurable retry limits
  - Detailed error logging

- **Dynamic Control**
  - Adjust pool size at runtime
  - Graceful resource reduction
  - Preserve running jobs during resizing

## Installation

1. Clone the repository:
```bash
git clone https://github.com/harlanhong/SlurmJobManager.git
cd SlurmJobManager
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```python
from job_manager import JobManager

# Create job manager instance
manager = JobManager(
    max_concurrent_jobs=2,
    check_interval=30.0,    # Check job status every 30 seconds
    print_interval=60.0,    # Print status every 60 seconds
    verbose=True,           # Enable status printing
    log_file="jobs.log",    # Log file path
    daemon=False            # Run in foreground mode
)

# Add a simple Python job
manager.add_job(
    job_id="python_task",
    script_path="process_data.py",
    args={
        "input_file": "data.txt",
        "output_file": "result.txt"
    },
    partition="gpu",
    num_gpus=1,
    num_cpus=4,
    memory="32G",
    conda_env="myenv",
    working_dir="/path/to/data"
)

# Run the manager
manager.run()
```

### Using with Blender

```python
# Add a Blender rendering job
manager.add_job(
    job_id="render_task",
    script_path="render_scene.py",
    args={
        "animation_blendfile": "/path/to/scene.blend",
        "output": "/path/to/output",
        "start_frame": "1",
        "end_frame": "100",
        "num_views": "6000"
    },
    partition="gpu",
    num_gpus=1,
    num_cpus=32,
    memory="200G",
    time_limit="4800:00:00",
    mail_type="END,FAIL",
    mail_user="your.email@example.com",
    executor="/path/to/blender",
    conda_env="blender_env",
    working_dir="/path/to/workspace"
)
```

### Running Modes

#### Foreground Mode (Default)
```python
manager = JobManager(
    verbose=True,           # Enable console output
    log_file="jobs.log"     # Optional: Also write to log file
)
manager.run()
```

#### Background Mode
```python
manager = JobManager(
    verbose=False,          # Disable console output
    log_file="jobs.log",    # Required for background mode
    daemon=True             # Enable background mode
)
manager.run()
```

### Status Monitoring

The manager will periodically print/log status information like this:

```
=== Job Status Update (2024-01-20 15:30:45) ===

Active Jobs:
  - render_task (Slurm ID: 12345):
    Status: RUNNING
    Runtime: 1:23:45
    Resources: gpu, 1GPU, 200G memory

Queued Jobs:
  - python_task (Will use: gpu, 1GPU, 32G memory)

Completed Jobs:
  - previous_task: Runtime 2:45:30

Job Statistics:
  Running: 1
  Queued: 1
  Completed: 1
  Failed: 0
  Cancelled: 0
==================================================

# Status information will be:
# - Printed to console in foreground mode (if verbose=True)
# - Written to log file (if log_file is specified)
# - Both (if both options are enabled)
```

## Advanced Features

### Dynamic Pool Size Adjustment

You can adjust the task pool size at runtime without interrupting running jobs:

```python
# 方法1：直接调用resize_pool方法
manager.resize_pool(new_size=1)  # 将池大小调整为1

# 方法2：如果在其他进程中调整
import os
import signal

# 将新的大小写入临时文件
with open("/tmp/slurm_pool_size", "w") as f:
    f.write("1")

# 发送SIGUSR1信号给管理器进程
os.kill(manager_pid, signal.SIGUSR1)
```

调整池大小时：
- 已运行的任务会继续执行直到完成
- 新的池大小限制仅影响新任务的提交
- 可以随时增加或减少池大小
- 所有更改都会记录到日志中

### SBATCH Configuration

You can specify detailed SBATCH parameters:

```python
manager.add_job(
    job_id="custom_task",
    script_path="script.py",
    args={"param1": "value1"},
    time_limit="24:00:00",
    mail_type="END,FAIL",
    mail_user="your.email@example.com",
    log_dir="/path/to/logs",
    extra_sbatch_params={
        "constraint": "some-constraint",
        "exclusive": ""
    }
)
```

### Custom Executor Support

The manager supports any custom executor program:

```python
manager.add_job(
    job_id="custom_task",
    script_path="script.py",
    executor="custom_program",
    executor_args=["-v", "--mode=batch"],
    script_args_separator="--"  # Specify how to separate script arguments
)
```

## Configuration Options

### JobManager Parameters

- `max_concurrent_jobs`: Maximum number of jobs running simultaneously
- `check_interval`: Time between job status checks (seconds)
- `max_retries`: Maximum number of retry attempts for failed jobs
- `print_interval`: Time between status prints (seconds)
- `verbose`: Enable/disable console output
- `log_file`: Path to log file (required for daemon mode)
- `daemon`: Run in background mode (requires log_file)

### Job Parameters

- `job_id`: Unique identifier for the job
- `script_path`: Path to the script to execute
- `args`: Dictionary of script arguments
- `partition`: Slurm partition name
- `num_gpus`: Number of GPUs required
- `num_cpus`: Number of CPU cores required
- `memory`: Memory requirement
- `conda_env`: Conda environment name
- `working_dir`: Working directory
- `executor`: Program to execute the script
- `executor_args`: Additional executor arguments
- `time_limit`: Job time limit
- `mail_type`: Email notification types
- `mail_user`: Email address for notifications
- `log_dir`: Directory for log files
- `extra_sbatch_params`: Additional SBATCH parameters

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

Fating Hong (fatinghong@gmail.com)