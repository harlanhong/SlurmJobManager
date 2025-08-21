from job_manager import JobManager
import time

def main():
    # 创建任务管理器实例
    manager = JobManager(
        max_concurrent_jobs=2,
        check_interval=30.0,    # 每30秒检查一次任务状态
        max_retries=2,          # 失败任务最多重试2次
        print_interval=60.0,    # 每60秒打印一次状态信息
        verbose=True            # 启用状态打印
    )
    
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