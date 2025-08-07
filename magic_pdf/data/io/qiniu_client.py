"""
七牛云存储客户端实现
支持文件上传、下载、删除等基本操作
"""

import os
import time
import yaml
from typing import Optional, Dict, Any, Union
from pathlib import Path
from qiniu import Auth, put_file, put_data, BucketManager, build_batch_copy
import qiniu.config as qiniu_config
from loguru import logger


class QiniuOSSClient:
    """七牛云存储客户端"""
    
    def __init__(self, config_path: str = None):
        """
        初始化七牛云客户端
        
        Args:
            config_path: 配置文件路径
        """
        if config_path is None:
            # 默认配置文件在项目根目录
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            config_path = os.path.join(project_root, "qiniu_config.yaml")
        self.config = self._load_config(config_path)
        self._setup_client()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """加载配置文件"""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        qiniu_config = config.get('qiniu', {})
        
        # 验证必要的配置项
        required_keys = ['access_key', 'secret_key', 'bucket_name']
        for key in required_keys:
            if not qiniu_config.get(key):
                raise ValueError(f"Missing required configuration: qiniu.{key}")
        
        return qiniu_config
    
    def _setup_client(self):
        """设置七牛云客户端"""
        # 认证信息
        self.auth = Auth(self.config['access_key'], self.config['secret_key'])
        
        # 存储配置
        self.bucket_name = self.config['bucket_name']
        
        # 存储管理器 - 现代版本的七牛云SDK的BucketManager不再需要config参数
        self.bucket_manager = BucketManager(self.auth)
        
        # CDN域名
        self.domain = self.config.get('domain', '')
        
        logger.info(f"七牛云客户端初始化成功，存储空间: {self.bucket_name}")
    
    def generate_upload_token(self, key: Optional[str] = None, expires: Optional[int] = None) -> str:
        """
        生成上传凭证
        
        Args:
            key: 文件保存的key，如果为None则表示不指定key
            expires: 凭证有效期（秒），默认使用配置文件中的设置
            
        Returns:
            上传凭证token
        """
        if expires is None:
            expires = self.config.get('upload_policy', {}).get('expires', 3600)
        
        # 构建上传策略
        policy = {
            'scope': self.bucket_name if key is None else f"{self.bucket_name}:{key}",
            'deadline': int(time.time()) + expires,
        }
        
        # 添加文件大小限制
        upload_policy = self.config.get('upload_policy', {})
        if upload_policy.get('fsizeLimit', 0) > 0:
            policy['fsizeLimit'] = upload_policy['fsizeLimit']
        
        # 添加文件类型限制
        if upload_policy.get('mime_limit'):
            policy['mimeLimit'] = upload_policy['mime_limit']
        
        return self.auth.upload_token(self.bucket_name, key, expires, policy)
    
    def upload_file(self, local_file_path: str, remote_key: str, 
                   progress_handler=None) -> Dict[str, Any]:
        """
        上传文件
        
        Args:
            local_file_path: 本地文件路径
            remote_key: 远程存储的key
            progress_handler: 进度回调函数
            
        Returns:
            上传结果字典
        """
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Local file not found: {local_file_path}")
        
        # 生成上传凭证
        token = self.generate_upload_token(remote_key)
        
        # 上传文件 - 现代版本的七牛云SDK的put_file不再需要config参数
        ret, info = put_file(token, remote_key, local_file_path, 
                           progress_handler=progress_handler)
        
        if info.status_code == 200:
            logger.info(f"文件上传成功: {local_file_path} -> {remote_key}")
            return {
                'success': True,
                'key': remote_key,
                'hash': ret.get('hash'),
                'size': os.path.getsize(local_file_path),
                'url': self.get_public_url(remote_key) if self.domain else None
            }
        else:
            logger.error(f"文件上传失败: {info}")
            return {
                'success': False,
                'error': info.text_body,
                'status_code': info.status_code
            }
    
    def upload_data(self, data: Union[str, bytes], remote_key: str) -> Dict[str, Any]:
        """
        上传数据
        
        Args:
            data: 要上传的数据（字符串或字节）
            remote_key: 远程存储的key
            
        Returns:
            上传结果字典
        """
        # 生成上传凭证
        token = self.generate_upload_token(remote_key)
        
        # 转换数据格式
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        # 上传数据 - 现代版本的七牛云SDK的put_data不再需要config参数
        ret, info = put_data(token, remote_key, data)
        
        if info.status_code == 200:
            logger.info(f"数据上传成功: {remote_key}")
            return {
                'success': True,
                'key': remote_key,
                'hash': ret.get('hash'),
                'size': len(data),
                'url': self.get_public_url(remote_key) if self.domain else None
            }
        else:
            logger.error(f"数据上传失败: {info}")
            return {
                'success': False,
                'error': info.text_body,
                'status_code': info.status_code
            }
    
    def download_file(self, remote_key: str, local_file_path: str) -> Dict[str, Any]:
        """
        下载文件
        
        Args:
            remote_key: 远程文件的key
            local_file_path: 本地保存路径
            
        Returns:
            下载结果字典
        """
        try:
            # 获取下载URL
            download_url = self.get_download_url(remote_key)
            
            # 使用urllib下载文件
            import urllib.request
            
            # 创建目录
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            
            # 下载文件
            urllib.request.urlretrieve(download_url, local_file_path)
            
            logger.info(f"文件下载成功: {remote_key} -> {local_file_path}")
            return {
                'success': True,
                'key': remote_key,
                'local_path': local_file_path,
                'size': os.path.getsize(local_file_path)
            }
            
        except Exception as e:
            logger.error(f"文件下载失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_public_url(self, remote_key: str) -> str:
        """
        获取公开访问URL
        
        Args:
            remote_key: 远程文件的key
            
        Returns:
            公开访问URL
        """
        if not self.domain:
            raise ValueError("Domain not configured for public URL generation")
        
        protocol = 'https' if self.config.get('download', {}).get('https', False) else 'http'
        return f"{protocol}://{self.domain}/{remote_key}"
    
    def get_private_url(self, remote_key: str, expires: Optional[int] = None) -> str:
        """
        获取私有访问URL（带签名）
        
        Args:
            remote_key: 远程文件的key
            expires: URL有效期（秒），默认使用配置文件中的设置
            
        Returns:
            私有访问URL
        """
        if not self.domain:
            raise ValueError("Domain not configured for private URL generation")
        
        if expires is None:
            expires = self.config.get('download', {}).get('expires', 3600)
        
        base_url = self.get_public_url(remote_key)
        return self.auth.private_download_url(base_url, expires=expires)
    
    def get_download_url(self, remote_key: str, expires: Optional[int] = None) -> str:
        """
        获取下载URL（自动判断公开或私有）
        
        Args:
            remote_key: 远程文件的key
            expires: URL有效期（秒），仅对私有链接有效
            
        Returns:
            下载URL
        """
        # 这里简化处理，默认返回私有链接
        # 实际使用中可以根据存储空间的权限设置来判断
        return self.get_private_url(remote_key, expires)
    
    def delete_file(self, remote_key: str) -> Dict[str, Any]:
        """
        删除文件
        
        Args:
            remote_key: 远程文件的key
            
        Returns:
            删除结果字典
        """
        ret, info = self.bucket_manager.delete(self.bucket_name, remote_key)
        
        if info.status_code == 200:
            logger.info(f"文件删除成功: {remote_key}")
            return {
                'success': True,
                'key': remote_key
            }
        else:
            logger.error(f"文件删除失败: {info}")
            return {
                'success': False,
                'error': info.text_body,
                'status_code': info.status_code
            }
    
    def list_files(self, prefix: str = '', limit: int = 100, marker: str = '') -> Dict[str, Any]:
        """
        列出文件
        
        Args:
            prefix: 文件前缀过滤
            limit: 返回的文件数量限制
            marker: 分页标记
            
        Returns:
            文件列表结果字典
        """
        ret, eof, info = self.bucket_manager.list(
            self.bucket_name, prefix=prefix, limit=limit, marker=marker
        )
        
        if info.status_code == 200:
            files = []
            for item in ret.get('items', []):
                files.append({
                    'key': item['key'],
                    'size': item['fsize'],
                    'hash': item['hash'],
                    'put_time': item['putTime'],
                    'mime_type': item.get('mimeType', ''),
                    'url': self.get_public_url(item['key']) if self.domain else None
                })
            
            return {
                'success': True,
                'files': files,
                'is_end': eof,
                'marker': ret.get('marker', ''),
                'total': len(files)
            }
        else:
            logger.error(f"文件列表获取失败: {info}")
            return {
                'success': False,
                'error': info.text_body,
                'status_code': info.status_code
            }
    
    def file_exists(self, remote_key: str) -> bool:
        """
        检查文件是否存在
        
        Args:
            remote_key: 远程文件的key
            
        Returns:
            文件是否存在
        """
        ret, info = self.bucket_manager.stat(self.bucket_name, remote_key)
        return info.status_code == 200
    
    def get_file_info(self, remote_key: str) -> Dict[str, Any]:
        """
        获取文件信息
        
        Args:
            remote_key: 远程文件的key
            
        Returns:
            文件信息字典
        """
        ret, info = self.bucket_manager.stat(self.bucket_name, remote_key)
        
        if info.status_code == 200:
            return {
                'success': True,
                'key': remote_key,
                'size': ret['fsize'],
                'hash': ret['hash'],
                'put_time': ret['putTime'],
                'mime_type': ret.get('mimeType', ''),
                'url': self.get_public_url(remote_key) if self.domain else None
            }
        else:
            return {
                'success': False,
                'error': info.text_body,
                'status_code': info.status_code
            }
    
    def copy_file(self, src_key: str, dest_key: str) -> Dict[str, Any]:
        """
        复制文件
        
        Args:
            src_key: 源文件key
            dest_key: 目标文件key
            
        Returns:
            复制结果字典
        """
        ret, info = self.bucket_manager.copy(
            self.bucket_name, src_key, self.bucket_name, dest_key
        )
        
        if info.status_code == 200:
            logger.info(f"文件复制成功: {src_key} -> {dest_key}")
            return {
                'success': True,
                'src_key': src_key,
                'dest_key': dest_key
            }
        else:
            logger.error(f"文件复制失败: {info}")
            return {
                'success': False,
                'error': info.text_body,
                'status_code': info.status_code
            }
    
    def move_file(self, src_key: str, dest_key: str) -> Dict[str, Any]:
        """
        移动文件
        
        Args:
            src_key: 源文件key
            dest_key: 目标文件key
            
        Returns:
            移动结果字典
        """
        ret, info = self.bucket_manager.move(
            self.bucket_name, src_key, self.bucket_name, dest_key
        )
        
        if info.status_code == 200:
            logger.info(f"文件移动成功: {src_key} -> {dest_key}")
            return {
                'success': True,
                'src_key': src_key,
                'dest_key': dest_key
            }
        else:
            logger.error(f"文件移动失败: {info}")
            return {
                'success': False,
                'error': info.text_body,
                'status_code': info.status_code
            }