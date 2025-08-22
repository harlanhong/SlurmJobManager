import subprocess
from typing import Dict, List, Optional, Tuple
import re
from datetime import datetime
import json

class ClusterInfo:
    def __init__(self):
        """初始化集群信息管理器"""
        self.partitions = {}  # 分区信息缓存
        self.nodes = {}      # 节点信息缓存
        self.last_update = None  # 最后更新时间
        self.update_interval = 60  # 更新间隔（秒）

    def _parse_sinfo_output(self, output: str) -> Dict:
        """解析sinfo命令输出"""
        partitions = {}
        lines = output.strip().split('\n')[1:]  # 跳过标题行
        
        for line in lines:
            parts = line.split()
            if len(parts) >= 8:
                partition = parts[0]
                if partition not in partitions:
                    partitions[partition] = {
                        'total_nodes': 0,
                        'available_nodes': 0,
                        'total_cpus': 0,
                        'available_cpus': 0,
                        'total_gpus': 0,
                        'available_gpus': 0,
                        'memory': 0,
                        'nodes': []
                    }
                
                node_info = {
                    'name': parts[5],
                    'state': parts[4],
                    'cpus': int(parts[3]),
                    'memory': self._parse_memory(parts[6])
                }
                
                partitions[partition]['nodes'].append(node_info)
                partitions[partition]['total_nodes'] += 1
                if 'alloc' not in node_info['state'].lower():
                    partitions[partition]['available_nodes'] += 1
                partitions[partition]['total_cpus'] += node_info['cpus']
                partitions[partition]['memory'] = max(partitions[partition]['memory'], node_info['memory'])
        
        return partitions

    def _parse_memory(self, mem_str: str) -> int:
        """解析内存字符串（例如：32G）为MB"""
        match = re.match(r'(\d+)([MGT])', mem_str)
        if not match:
            return 0
        
        value, unit = match.groups()
        value = int(value)
        if unit == 'T':
            return value * 1024 * 1024
        elif unit == 'G':
            return value * 1024
        return value

    def _get_gpu_info(self) -> Dict[str, Dict]:
        """获取GPU信息"""
        try:
            # 使用scontrol show node获取更详细的节点信息
            result = subprocess.run(['scontrol', 'show', 'node'], 
                                 capture_output=True, text=True, check=True)
            
            nodes = {}
            current_node = None
            
            for line in result.stdout.split('\n'):
                if line.startswith('NodeName='):
                    if current_node:
                        nodes[current_node['name']] = current_node
                    current_node = {'name': line.split('=')[1].split(' ')[0]}
                elif current_node and 'Gres=' in line:
                    # 解析GPU信息
                    gres_parts = line.split('Gres=')[1].split(',')
                    for part in gres_parts:
                        if 'gpu' in part.lower():
                            gpu_match = re.search(r'gpu:(\d+)', part.lower())
                            if gpu_match:
                                current_node['gpus'] = int(gpu_match.group(1))
            
            if current_node:
                nodes[current_node['name']] = current_node
                
            return nodes
        except subprocess.CalledProcessError:
            return {}

    def update(self) -> None:
        """更新集群信息"""
        now = datetime.now()
        if (self.last_update and 
            (now - self.last_update).total_seconds() < self.update_interval):
            return
        
        try:
            # 获取分区和节点信息
            result = subprocess.run(['sinfo', '-o', '%P %a %l %D %T %N %C %m'],
                                 capture_output=True, text=True, check=True)
            self.partitions = self._parse_sinfo_output(result.stdout)
            
            # 获取GPU信息
            gpu_info = self._get_gpu_info()
            
            # 更新GPU信息到分区信息中
            for partition in self.partitions.values():
                for node in partition['nodes']:
                    if node['name'] in gpu_info:
                        node['gpus'] = gpu_info[node['name']].get('gpus', 0)
                        partition['total_gpus'] += node['gpus']
                        if 'alloc' not in node['state'].lower():
                            partition['available_gpus'] += node['gpus']
            
            self.last_update = now
        except subprocess.CalledProcessError as e:
            print(f"更新集群信息失败: {e}")

    def get_partition_info(self, partition_name: str) -> Optional[Dict]:
        """获取指定分区的信息"""
        self.update()
        return self.partitions.get(partition_name)

    def get_all_partitions(self) -> Dict:
        """获取所有分区的信息"""
        self.update()
        return self.partitions

    def get_resource_summary(self) -> Dict:
        """获取资源使用摘要"""
        self.update()
        summary = {
            'total_nodes': 0,
            'available_nodes': 0,
            'total_cpus': 0,
            'available_cpus': 0,
            'total_gpus': 0,
            'available_gpus': 0,
            'partitions': len(self.partitions)
        }
        
        for partition in self.partitions.values():
            summary['total_nodes'] += partition['total_nodes']
            summary['available_nodes'] += partition['available_nodes']
            summary['total_cpus'] += partition['total_cpus']
            summary['total_gpus'] += partition['total_gpus']
            summary['available_gpus'] += partition['available_gpus']
        
        return summary

    def check_resource_availability(self, partition: str, cpus: int, gpus: int, 
                                 memory: str) -> Tuple[bool, str]:
        """
        检查资源是否可用
        
        Args:
            partition: 分区名称
            cpus: 需要的CPU核数
            gpus: 需要的GPU数量
            memory: 需要的内存（例如：'32G'）
        
        Returns:
            (是否可用, 原因说明)
        """
        self.update()
        
        if partition not in self.partitions:
            return False, f"分区 {partition} 不存在"
        
        partition_info = self.partitions[partition]
        memory_mb = self._parse_memory(memory)
        
        if partition_info['available_nodes'] == 0:
            return False, f"分区 {partition} 没有可用节点"
        
        if cpus > max(node['cpus'] for node in partition_info['nodes']):
            return False, f"没有节点有足够的CPU核心（需要 {cpus}）"
        
        if gpus > 0:
            max_gpus = max((node.get('gpus', 0) for node in partition_info['nodes']), default=0)
            if gpus > max_gpus:
                return False, f"没有节点有足够的GPU（需要 {gpus}）"
            if partition_info['available_gpus'] < gpus:
                return False, f"没有足够的可用GPU（需要 {gpus}，可用 {partition_info['available_gpus']}）"
        
        if memory_mb > partition_info['memory']:
            return False, f"没有节点有足够的内存（需要 {memory}）"
        
        return True, "资源可用"

    def to_json(self) -> str:
        """将集群信息转换为JSON格式"""
        self.update()
        return json.dumps({
            'partitions': self.partitions,
            'summary': self.get_resource_summary(),
            'last_update': self.last_update.isoformat() if self.last_update else None
        }, ensure_ascii=False)
