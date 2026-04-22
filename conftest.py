"""
Pytest configuration for fire_100UpPlan tests
Used by both local pytest and GitHub Actions CI
"""
import os
import sys

# ─── Django Settings ─────────────────────────────────
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'firePlanProject.settings')

# ─── pytest-django markers ───────────────────────────
def pytest_configure(config):
    """Register custom markers for test categorization"""
    config.addinivalue_line("markers", "unit: Unit tests (fast, no external deps)")
    config.addinivalue_line("markers", "integration: Integration tests (require DB, Redis)")
    config.addinivalue_line("markers", "e2e: End-to-end API journey tests")
    config.addinivalue_line("markers", "performance: Performance/load tests")
    config.addinivalue_line("markers", "slow: Slow-running tests (>10s)")
