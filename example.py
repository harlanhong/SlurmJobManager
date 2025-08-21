from job_manager import JobManager
import time

def resize_pool_example():
    """展示如何在运行时调整任务池大小"""
    import threading
    import time
    
    def resize_after_delay():
        # 等待30秒后将池大小调整为1
        time.sleep(30)
        print("\n正在将任务池大小调整为1...")
        manager.resize_pool(1)
        
        # 再等待30秒后将池大小调整回2
        time.sleep(30)
        print("\n正在将任务池大小调整回2...")
        manager.resize_pool(2)
    
    # 创建任务管理器实例（前台模式）
    manager = JobManager(
        max_concurrent_jobs=2,
        check_interval=30.0,    # 每30秒检查一次任务状态
        max_retries=2,          # 失败任务最多重试2次
        print_interval=60.0,    # 每60秒打印一次状态信息
        verbose=True,           # 启用控制台输出
        log_file="jobs.log",    # 同时写入日志文件
        daemon=False            # 前台运行模式
    )
    
    # 启动一个线程在30秒后调整池大小
    resize_thread = threading.Thread(target=resize_after_delay)
    resize_thread.daemon = True
    resize_thread.start()
    
    # 或者使用后台模式
    """
    manager = JobManager(
        max_concurrent_jobs=2,
        check_interval=30.0,
        max_retries=2,
        print_interval=60.0,
        verbose=False,          # 禁用控制台输出
        log_file="jobs.log",    # 必须指定日志文件
        daemon=True             # 后台运行模式
    )
    """
    
    # 示例1：使用Python执行器（默认）
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
    
    # 示例2：使用Blender执行器（复杂配置）
    manager.add_job(
        job_id="gen_dynamic_w_hair_multi_viewslurm",
        script_path="src/render_human_multiview.py",
        args={
            "animation_blendfile": "/mnt/users_scratch/fatinghong/workspace/data/digital_humans/dynamic_with_hair/id00048/postprocessing/animation/animation_id00048_illumination.blend",
            "simulation_dir": "/mnt/users_scratch/fatinghong/workspace/data/digital_humans/dynamic_with_hair/id00048/postprocessing/sim_output",
            "save_dir": "/mnt/users_scratch/fatinghong/workspace/data/digital_humans/dynamic_with_hair/id00048/postprocessing/multiview_dynamic_renders_delete",
            "start_frame": "1",
            "end_frame": "118",
            "export_body": "",
            "num_views": "6000"
        },
        partition="GeminiAsh",
        num_gpus=1,
        num_cpus=32,
        memory="200G",
        time_limit="4800:00:00",
        mail_type="END,FAIL",
        mail_user="fatinghong@gmail.com",
        log_dir="/home/fatinghong/workspace/src/VirtualHair/slurm_log",
        executor="/mnt/users_scratch/fatinghong/workspace/blender43_linux_v4/bin/blender",
        working_dir="/mnt/users_scratch/fatinghong/workspace/src/VirtualHair/",
        conda_env="virtualhair"
    )
    
    try:
        # 启动任务管理器
        manager.run()
        
        # 获取所有任务的最终状态
        all_status = manager.get_all_jobs_status()
        print("\n最终任务状态:")
        for job_id, status in all_status.items():
            print(f"任务 {job_id}: {status}")
            
    except KeyboardInterrupt:
        print("\n用户中断，正在清理...")
        manager.cancel_all_jobs()

if __name__ == "__main__":
    main()