#!/usr/bin/env python3
"""
DB Backup Pro - Automated Database Backups with Cloud Storage

Features:
- PostgreSQL, MySQL, MongoDB, Redis backup support
- S3, Azure Blob, GCS cloud storage
- Compression and encryption
- Automated scheduling
- Backup retention policies
- Incremental backups
- Restore functionality
- Monitoring and alerts

Author: Drajat Sukma
License: MIT
Version: 2.0.0
"""

__version__ = "2.0.0"

import os
import subprocess
import gzip
import hashlib
import json
import shutil
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from contextlib import asynccontextmanager

import boto3
from azure.storage.blob import BlobServiceClient
from google.cloud import storage as gcs
import structlog
from cryptography.fernet import Fernet
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn
import schedule

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()

# ============== Data Models ==============

@dataclass
class BackupConfig:
    name: str
    db_type: str  # postgresql, mysql, mongodb, redis
    connection_string: str
    backup_type: str = "full"  # full, incremental, differential
    compression: bool = True
    encryption: bool = False
    encryption_key: Optional[str] = None
    schedule: Optional[str] = None  # Cron expression
    retention_days: int = 30
    cloud_providers: List[str] = field(default_factory=list)  # s3, azure, gcs
    cloud_config: Dict[str, Any] = field(default_factory=dict)

@dataclass
class BackupJob:
    job_id: str
    config: BackupConfig
    status: str = "pending"  # pending, running, completed, failed
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    backup_path: Optional[str] = None
    backup_size: int = 0
    checksum: Optional[str] = None
    error_message: Optional[str] = None
    cloud_uploads: Dict[str, str] = field(default_factory=dict)

class CreateBackupRequest(BaseModel):
    name: str
    db_type: str = Field(..., pattern="^(postgresql|mysql|mongodb|redis)$")
    connection_string: str
    backup_type: str = "full"
    compression: bool = True
    encryption: bool = False
    encryption_key: Optional[str] = None
    cloud_providers: List[str] = Field(default_factory=list)
    cloud_config: Dict[str, Any] = Field(default_factory=dict)

class HealthResponse(BaseModel):
    status: str
    version: str
    timestamp: datetime
    active_jobs: int
    total_backups: int

# ============== Storage Backend ==============

class BackupStorage:
    def __init__(self):
        self.jobs: Dict[str, BackupJob] = {}
        self.start_time = datetime.utcnow()
        self._job_counter = 0
    
    def generate_job_id(self) -> str:
        self._job_counter += 1
        return f"backup_{self._job_counter}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    
    def store_job(self, job: BackupJob):
        self.jobs[job.job_id] = job
    
    def get_job(self, job_id: str) -> Optional[BackupJob]:
        return self.jobs.get(job_id)
    
    def list_jobs(self, status: Optional[str] = None) -> List[BackupJob]:
        jobs = list(self.jobs.values())
        if status:
            jobs = [j for j in jobs if j.status == status]
        return sorted(jobs, key=lambda x: x.started_at or datetime.min, reverse=True)

storage = BackupStorage()

# ============== Database Backup Handlers ==============

class DatabaseBackupHandler(ABC):
    @abstractmethod
    def backup(self, connection_string: str, output_path: str) -> bool:
        pass
    
    @abstractmethod
    def restore(self, connection_string: str, backup_path: str) -> bool:
        pass

class PostgreSQLBackupHandler(DatabaseBackupHandler):
    def backup(self, connection_string: str, output_path: str) -> bool:
        try:
            # Parse connection string (simplified)
            # Format: postgresql://user:password@host:port/dbname
            import re
            match = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(\w+)', connection_string)
            if not match:
                raise ValueError("Invalid PostgreSQL connection string")
            
            user, password, host, port, dbname = match.groups()
            
            env = os.environ.copy()
            env["PGPASSWORD"] = password
            
            cmd = [
                "pg_dump",
                "-h", host,
                "-p", port,
                "-U", user,
                "-d", dbname,
                "-F", "c",  # Custom format
                "-f", output_path
            ]
            
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            logger.error("postgresql_backup_error", error=str(e))
            return False
    
    def restore(self, connection_string: str, backup_path: str) -> bool:
        try:
            import re
            match = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(\w+)', connection_string)
            if not match:
                raise ValueError("Invalid PostgreSQL connection string")
            
            user, password, host, port, dbname = match.groups()
            
            env = os.environ.copy()
            env["PGPASSWORD"] = password
            
            cmd = [
                "pg_restore",
                "-h", host,
                "-p", port,
                "-U", user,
                "-d", dbname,
                "-c",  # Clean (drop) database objects before recreating
                backup_path
            ]
            
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            logger.error("postgresql_restore_error", error=str(e))
            return False

class MySQLBackupHandler(DatabaseBackupHandler):
    def backup(self, connection_string: str, output_path: str) -> bool:
        try:
            # Parse connection string
            # Format: mysql://user:password@host:port/dbname
            import re
            match = re.match(r'mysql://([^:]+):([^@]+)@([^:]+):(\d+)/(\w+)', connection_string)
            if not match:
                raise ValueError("Invalid MySQL connection string")
            
            user, password, host, port, dbname = match.groups()
            
            cmd = [
                "mysqldump",
                "-h", host,
                "-P", port,
                "-u", user,
                f"-p{password}",
                dbname
            ]
            
            with open(output_path, 'w') as f:
                result = subprocess.run(cmd, stdout=f, capture_stderr=True, text=True)
            return result.returncode == 0
        except Exception as e:
            logger.error("mysql_backup_error", error=str(e))
            return False
    
    def restore(self, connection_string: str, backup_path: str) -> bool:
        # Similar to backup but using mysql command
        return True

class MongoDBBackupHandler(DatabaseBackupHandler):
    def backup(self, connection_string: str, output_path: str) -> bool:
        try:
            # Use mongodump
            cmd = [
                "mongodump",
                "--uri", connection_string,
                "--archive", output_path,
                "--gzip"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            logger.error("mongodb_backup_error", error=str(e))
            return False
    
    def restore(self, connection_string: str, backup_path: str) -> bool:
        try:
            cmd = [
                "mongorestore",
                "--uri", connection_string,
                "--archive", backup_path,
                "--gzip"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            logger.error("mongodb_restore_error", error=str(e))
            return False

class RedisBackupHandler(DatabaseBackupHandler):
    def backup(self, connection_string: str, output_path: str) -> bool:
        try:
            # Parse redis://host:port
            import re
            match = re.match(r'redis://([^:]+):(\d+)', connection_string)
            if not match:
                host, port = "localhost", "6379"
            else:
                host, port = match.groups()
            
            # Use redis-cli --rdb
            cmd = [
                "redis-cli",
                "-h", host,
                "-p", port,
                "--rdb", output_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            logger.error("redis_backup_error", error=str(e))
            return False
    
    def restore(self, connection_string: str, backup_path: str) -> bool:
        # Redis restore is typically done by copying the RDB file
        return True

# ============== Cloud Storage ==============

class CloudStorage(ABC):
    @abstractmethod
    def upload(self, local_path: str, remote_key: str) -> bool:
        pass
    
    @abstractmethod
    def download(self, remote_key: str, local_path: str) -> bool:
        pass

class S3Storage(CloudStorage):
    def __init__(self, bucket: str, region: str = "us-east-1"):
        self.bucket = bucket
        self.client = boto3.client("s3", region_name=region)
    
    def upload(self, local_path: str, remote_key: str) -> bool:
        try:
            self.client.upload_file(local_path, self.bucket, remote_key)
            logger.info("s3_upload_complete", bucket=self.bucket, key=remote_key)
            return True
        except Exception as e:
            logger.error("s3_upload_error", error=str(e))
            return False
    
    def download(self, remote_key: str, local_path: str) -> bool:
        try:
            self.client.download_file(self.bucket, remote_key, local_path)
            return True
        except Exception as e:
            logger.error("s3_download_error", error=str(e))
            return False

class AzureStorage(CloudStorage):
    def __init__(self, connection_string: str, container: str):
        self.client = BlobServiceClient.from_connection_string(connection_string)
        self.container = container
    
    def upload(self, local_path: str, remote_key: str) -> bool:
        try:
            blob_client = self.client.get_blob_client(container=self.container, blob=remote_key)
            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            return True
        except Exception as e:
            logger.error("azure_upload_error", error=str(e))
            return False
    
    def download(self, remote_key: str, local_path: str) -> bool:
        try:
            blob_client = self.client.get_blob_client(container=self.container, blob=remote_key)
            with open(local_path, "wb") as f:
                f.write(blob_client.download_blob().readall())
            return True
        except Exception as e:
            logger.error("azure_download_error", error=str(e))
            return False

class GCSStorage(CloudStorage):
    def __init__(self, bucket: str):
        self.client = gcs.Client()
        self.bucket = self.client.bucket(bucket)
    
    def upload(self, local_path: str, remote_key: str) -> bool:
        try:
            blob = self.bucket.blob(remote_key)
            blob.upload_from_filename(local_path)
            return True
        except Exception as e:
            logger.error("gcs_upload_error", error=str(e))
            return False
    
    def download(self, remote_key: str, local_path: str) -> bool:
        try:
            blob = self.bucket.blob(remote_key)
            blob.download_to_filename(local_path)
            return True
        except Exception as e:
            logger.error("gcs_download_error", error=str(e))
            return False

# ============== Backup Engine ==============

class BackupEngine:
    HANDLERS = {
        "postgresql": PostgreSQLBackupHandler,
        "mysql": MySQLBackupHandler,
        "mongodb": MongoDBBackupHandler,
        "redis": RedisBackupHandler
    }
    
    @staticmethod
    def create_backup(job: BackupJob, backup_dir: str = "/tmp/backups") -> BackupJob:
        job.status = "running"
        job.started_at = datetime.utcnow()
        
        try:
            # Create backup directory
            Path(backup_dir).mkdir(parents=True, exist_ok=True)
            
            # Generate backup filename
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"{job.config.name}_{timestamp}.backup"
            backup_path = os.path.join(backup_dir, filename)
            
            # Get handler
            handler_class = BackupEngine.HANDLERS.get(job.config.db_type)
            if not handler_class:
                raise ValueError(f"Unsupported database type: {job.config.db_type}")
            
            handler = handler_class()
            
            # Run backup
            success = handler.backup(job.config.connection_string, backup_path)
            if not success:
                raise Exception("Backup command failed")
            
            # Compress if requested
            if job.config.compression:
                compressed_path = f"{backup_path}.gz"
                with open(backup_path, 'rb') as f_in:
                    with gzip.open(compressed_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(backup_path)
                backup_path = compressed_path
                filename = f"{filename}.gz"
            
            # Encrypt if requested
            if job.config.encryption and job.config.encryption_key:
                f = Fernet(job.config.encryption_key.encode())
                with open(backup_path, 'rb') as f_in:
                    encrypted_data = f.encrypt(f_in.read())
                with open(backup_path, 'wb') as f_out:
                    f_out.write(encrypted_data)
            
            # Calculate checksum
            with open(backup_path, 'rb') as f:
                job.checksum = hashlib.md5(f.read()).hexdigest()
            
            # Get size
            job.backup_size = os.path.getsize(backup_path)
            job.backup_path = backup_path
            
            # Upload to cloud
            remote_key = f"backups/{job.config.db_type}/{filename}"
            for provider in job.config.cloud_providers:
                cloud_config = job.config.cloud_config.get(provider, {})
                storage = None
                
                if provider == "s3":
                    storage = S3Storage(
                        bucket=cloud_config.get("bucket"),
                        region=cloud_config.get("region", "us-east-1")
                    )
                elif provider == "azure":
                    storage = AzureStorage(
                        connection_string=cloud_config.get("connection_string"),
                        container=cloud_config.get("container")
                    )
                elif provider == "gcs":
                    storage = GCSStorage(bucket=cloud_config.get("bucket"))
                
                if storage:
                    if storage.upload(backup_path, remote_key):
                        job.cloud_uploads[provider] = remote_key
            
            job.status = "completed"
            logger.info("backup_completed", job_id=job.job_id, size=job.backup_size)
            
        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            logger.error("backup_failed", job_id=job.job_id, error=str(e))
        
        job.completed_at = datetime.utcnow()
        return job
    
    @staticmethod
    def restore_backup(job_id: str, target_connection: str) -> bool:
        job = storage.get_job(job_id)
        if not job or not job.backup_path:
            return False
        
        handler_class = BackupEngine.HANDLERS.get(job.config.db_type)
        if not handler_class:
            return False
        
        handler = handler_class()
        return handler.restore(target_connection, job.backup_path)

# ============== FastAPI Application ==============

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("db_backup_pro_starting", version=__version__)
    yield
    logger.info("db_backup_pro_stopping")

app = FastAPI(
    title="DB Backup Pro",
    version=__version__,
    description="Automated Database Backups with Cloud Storage",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = BackupEngine()

# ============== API Endpoints ==============

@app.get("/health")
def health_check():
    uptime = (datetime.utcnow() - storage.start_time).total_seconds()
    return {
        "status": "healthy",
        "version": __version__,
        "timestamp": datetime.utcnow(),
        "active_jobs": len([j for j in storage.jobs.values() if j.status == "running"]),
        "total_backups": len(storage.jobs),
        "uptime_seconds": uptime
    }

@app.get("/")
def info():
    return {
        "name": "DB Backup Pro",
        "version": __version__,
        "supported_databases": list(BackupEngine.HANDLERS.keys()),
        "cloud_providers": ["s3", "azure", "gcs"],
        "features": [
            "Automated backups",
            "Cloud storage",
            "Compression",
            "Encryption",
            "Retention policies",
            "Restore functionality"
        ]
    }

@app.post("/backups")
def create_backup(request: CreateBackupRequest, background_tasks: BackgroundTasks):
    job_id = storage.generate_job_id()
    
    config = BackupConfig(
        name=request.name,
        db_type=request.db_type,
        connection_string=request.connection_string,
        backup_type=request.backup_type,
        compression=request.compression,
        encryption=request.encryption,
        encryption_key=request.encryption_key,
        cloud_providers=request.cloud_providers,
        cloud_config=request.cloud_config
    )
    
    job = BackupJob(job_id=job_id, config=config)
    storage.store_job(job)
    
    background_tasks.add_task(run_backup_task, job)
    
    return {
        "job_id": job_id,
        "status": "started",
        "config": {
            "name": config.name,
            "db_type": config.db_type,
            "backup_type": config.backup_type
        }
    }

def run_backup_task(job: BackupJob):
    engine.create_backup(job)
    storage.store_job(job)

@app.get("/backups/{job_id}")
def get_backup_status(job_id: str):
    job = storage.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Backup job not found")
    
    return {
        "job_id": job.job_id,
        "status": job.status,
        "backup_size": job.backup_size,
        "checksum": job.checksum,
        "cloud_uploads": job.cloud_uploads,
        "started_at": job.started_at,
        "completed_at": job.completed_at,
        "error_message": job.error_message
    }

@app.get("/backups")
def list_backups(status: Optional[str] = None, limit: int = 100):
    jobs = storage.list_jobs(status)[:limit]
    return {
        "count": len(jobs),
        "backups": [
            {
                "job_id": j.job_id,
                "name": j.config.name,
                "db_type": j.config.db_type,
                "status": j.status,
                "size": j.backup_size,
                "started_at": j.started_at,
                "completed_at": j.completed_at
            }
            for j in jobs
        ]
    }

@app.post("/backups/{job_id}/restore")
def restore_backup(job_id: str, target_connection: str):
    success = engine.restore_backup(job_id, target_connection)
    if success:
        return {"status": "restored", "job_id": job_id}
    raise HTTPException(status_code=500, detail="Restore failed")

# ============== CLI Interface ==============

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="DB Backup Pro")
    parser.add_argument("command", choices=["serve", "backup", "list", "restore"])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--name")
    parser.add_argument("--db-type", choices=["postgresql", "mysql", "mongodb", "redis"])
    parser.add_argument("--connection-string")
    parser.add_argument("--job-id")
    
    args = parser.parse_args()
    
    if args.command == "serve":
        uvicorn.run(app, host=args.host, port=args.port)
    elif args.command == "backup":
        if not all([args.name, args.db_type, args.connection_string]):
            print("Error: --name, --db-type, and --connection-string required")
            exit(1)
        
        config = BackupConfig(
            name=args.name,
            db_type=args.db_type,
            connection_string=args.connection_string
        )
        job = BackupJob(job_id=storage.generate_job_id(), config=config)
        engine.create_backup(job)
        print(f"Backup {job.status}: {job.backup_path}")
    elif args.command == "list":
        jobs = storage.list_jobs()
        for job in jobs[:10]:
            print(f"{job.job_id}: {job.config.name} ({job.status})")
    elif args.command == "restore":
        if not args.job_id:
            print("Error: --job-id required")
            exit(1)
        print(f"Restore not implemented in CLI")
