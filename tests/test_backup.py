"""Tests for DB Backup Pro"""

import pytest
from fastapi.testclient import TestClient

from backup import app, storage, BackupJob, BackupConfig, BackupEngine

client = TestClient(app)


class TestHealth:
    def test_health_check(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_info(self):
        response = client.get("/")
        assert response.status_code == 200
        assert "DB Backup Pro" in response.json()["name"]
        assert "postgresql" in response.json()["supported_databases"]


class TestBackupCreation:
    def test_create_backup(self):
        request = {
            "name": "test_backup",
            "db_type": "postgresql",
            "connection_string": "postgresql://user:pass@localhost:5432/testdb",
            "backup_type": "full",
            "compression": True,
            "cloud_providers": []
        }
        response = client.post("/backups", json=request)
        assert response.status_code == 200
        data = response.json()
        assert "job_id" in data
        assert data["status"] == "started"

    def test_create_backup_invalid_db_type(self):
        request = {
            "name": "test_backup",
            "db_type": "invalid",
            "connection_string": "postgresql://user:pass@localhost:5432/testdb"
        }
        response = client.post("/backups", json=request)
        assert response.status_code == 422

    def test_get_backup_status(self):
        # Create a mock job
        job_id = storage.generate_job_id()
        job = BackupJob(
            job_id=job_id,
            config=BackupConfig(name="test", db_type="postgresql", connection_string="test"),
            status="completed",
            backup_size=1024
        )
        storage.store_job(job)
        
        response = client.get(f"/backups/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["job_id"] == job_id
        assert data["status"] == "completed"

    def test_get_backup_not_found(self):
        response = client.get("/backups/nonexistent")
        assert response.status_code == 404

    def test_list_backups(self):
        response = client.get("/backups")
        assert response.status_code == 200
        data = response.json()
        assert "count" in data
        assert "backups" in data
