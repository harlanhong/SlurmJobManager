from job_manager import JobManager
import time

def main():
    # 创建任务管理器实例
    manager = JobManager(
        max_concurrent_jobs=2,
        check_interval=30.0,    # 每30秒检查一次任务状态
        max_retries=2,          # 失败任务最多重试2次
        print_interval=60.0,    # 每60秒打印一次状态信息
        verbose=True,           # 启用控制台输出
        log_file="jobs.log",    # 同时写入日志文件
        daemon=False            # 前台运行模式
    )
    
    # 添加一些测试任务
    for i in range(5):
        manager.add_job(
            job_id=f"test_task_{i}",
            script_path="test_script.py",
            args={
                "input": f"input_{i}.txt",
                "output": f"output_{i}.txt"
            },
            partition="gpu",
            num_gpus=1,
            num_cpus=4,
            memory="16G",
            conda_env="test_env",
            working_dir="/path/to/work"
        )
    
    try:
        # 启动任务管理器（启用Web监控）
        manager.run(
            web_monitor=True,        # 启用Web监控
            web_host='127.0.0.1',    # 监听本地地址
            web_port=5000            # 使用5000端口
        )
    except KeyboardInterrupt:
        print("\n用户中断，正在清理...")
        manager.cancel_all_jobs()

if __name__ == "__main__":
    main()
