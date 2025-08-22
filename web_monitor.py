from flask import Flask, render_template
from flask_socketio import SocketIO, emit
import json
import threading
import time
from typing import Optional
from datetime import datetime
import pytz

app = Flask(__name__)
socketio = SocketIO(app)

# 全局变量存储JobManager实例
job_manager = None

@app.route('/')
def index():
    """渲染主页"""
    return render_template('index.html')

@socketio.on('connect')
def handle_connect():
    """处理客户端连接"""
    if job_manager:
        # 发送初始状态
        emit('status_update', _get_status_data())

def _get_status_data():
    """获取任务状态数据"""
    if not job_manager:
        return {}
    
    status = job_manager.get_all_jobs_status()
    
    # 按状态分组
    groups = {
        "running": [],
        "queued": [],
        "completed": [],
        "failed": [],
        "cancelled": []
    }
    
    for job_id, info in status.items():
        job_status = info["status"].lower()
        if job_status == "running":
            groups["running"].append({
                "id": job_id,
                "slurm_id": info["slurm_id"],
                "runtime": info["runtime"],
                "resources": f"{info['partition']}, {info['num_gpus']}GPU, {info['memory']}内存"
            })
        elif job_status == "queued":
            groups["queued"].append({
                "id": job_id,
                "resources": f"{info['partition']}, {info['num_gpus']}GPU, {info['memory']}内存"
            })
        elif job_status == "completed":
            groups["completed"].append({
                "id": job_id,
                "runtime": info["runtime"]
            })
        elif job_status == "failed":
            groups["failed"].append({
                "id": job_id,
                "retry_count": info["retry_count"]
            })
        elif job_status == "cancelled":
            groups["cancelled"].append({
                "id": job_id
            })
    
    # 添加统计信息
    stats = {
        "running": len(groups["running"]),
        "queued": len(groups["queued"]),
        "completed": len(groups["completed"]),
        "failed": len(groups["failed"]),
        "cancelled": len(groups["cancelled"])
    }
    
    return {
        "groups": groups,
        "stats": stats,
        "timestamp": datetime.now(pytz.timezone('Europe/Zurich')).strftime("%Y-%m-%d %H:%M:%S %Z")
    }

def status_update_thread():
    """定期发送状态更新"""
    while True:
        if job_manager:
            socketio.emit('status_update', _get_status_data())
        time.sleep(1)  # 每秒更新一次

def run_monitor(manager, host: str = '127.0.0.1', port: int = 5000):
    """
    运行Web监控服务器
    
    Args:
        manager: JobManager实例
        host: 监听地址
        port: 监听端口
    """
    global job_manager
    job_manager = manager
    
    # 启动状态更新线程
    update_thread = threading.Thread(target=status_update_thread)
    update_thread.daemon = True
    update_thread.start()
    
    # 启动Web服务器
    socketio.run(app, host=host, port=port)
