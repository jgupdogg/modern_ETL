#!/usr/bin/env python3
"""
MinIO Test Script

This script tests MinIO connectivity and basic operations:
- Connection to MinIO
- Bucket creation/listing
- Object upload/download/deletion
- Error handling
"""

import os
import sys
import json
import asyncio
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import io

# MinIO configuration from environment variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Test configuration
TEST_BUCKET = "test-bucket"
TEST_OBJECT = "test-object.txt"
TEST_CONTENT = f"This is a test file created at {datetime.now().isoformat()}"


def get_minio_client():
    """Create and return MinIO client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )


def test_connection(client):
    """Test MinIO connection."""
    try:
        # List buckets to test connection
        buckets = client.list_buckets()
        print(f"✅ Connected to MinIO at {MINIO_ENDPOINT}")
        print(f"   Found {len(buckets)} existing bucket(s)")
        for bucket in buckets:
            print(f"   - {bucket.name} (created: {bucket.creation_date})")
        return True
    except Exception as e:
        print(f"❌ Failed to connect to MinIO: {e}")
        return False


def test_bucket_operations(client):
    """Test bucket operations."""
    try:
        # Check if bucket exists
        if client.bucket_exists(TEST_BUCKET):
            print(f"✅ Bucket '{TEST_BUCKET}' already exists")
        else:
            # Create bucket
            client.make_bucket(TEST_BUCKET)
            print(f"✅ Created bucket '{TEST_BUCKET}'")
        
        # List buckets
        buckets = [b.name for b in client.list_buckets()]
        assert TEST_BUCKET in buckets, f"Bucket {TEST_BUCKET} not found in bucket list"
        print(f"✅ Bucket '{TEST_BUCKET}' verified in bucket list")
        
        return True
    except Exception as e:
        print(f"❌ Bucket operations failed: {e}")
        return False


def test_object_operations(client):
    """Test object operations."""
    try:
        # Upload object
        data = io.BytesIO(TEST_CONTENT.encode('utf-8'))
        data_size = len(TEST_CONTENT.encode('utf-8'))
        
        client.put_object(
            TEST_BUCKET,
            TEST_OBJECT,
            data,
            data_size,
            content_type='text/plain'
        )
        print(f"✅ Uploaded object '{TEST_OBJECT}' to bucket '{TEST_BUCKET}'")
        
        # List objects
        objects = list(client.list_objects(TEST_BUCKET))
        object_names = [obj.object_name for obj in objects]
        assert TEST_OBJECT in object_names, f"Object {TEST_OBJECT} not found in bucket"
        print(f"✅ Object '{TEST_OBJECT}' verified in bucket listing")
        
        # Download object
        response = client.get_object(TEST_BUCKET, TEST_OBJECT)
        downloaded_content = response.read().decode('utf-8')
        response.close()
        
        assert downloaded_content == TEST_CONTENT, "Downloaded content doesn't match uploaded content"
        print(f"✅ Downloaded and verified object content")
        print(f"   Content: {downloaded_content[:50]}...")
        
        # Get object stats
        stat = client.stat_object(TEST_BUCKET, TEST_OBJECT)
        print(f"✅ Object stats:")
        print(f"   - Size: {stat.size} bytes")
        print(f"   - ETag: {stat.etag}")
        print(f"   - Content-Type: {stat.content_type}")
        print(f"   - Last Modified: {stat.last_modified}")
        
        return True
    except Exception as e:
        print(f"❌ Object operations failed: {e}")
        return False


def test_cleanup(client):
    """Clean up test objects."""
    try:
        # Remove test object
        client.remove_object(TEST_BUCKET, TEST_OBJECT)
        print(f"✅ Removed test object '{TEST_OBJECT}'")
        
        # Verify object is removed
        objects = list(client.list_objects(TEST_BUCKET))
        object_names = [obj.object_name for obj in objects]
        assert TEST_OBJECT not in object_names, f"Object {TEST_OBJECT} still exists"
        print(f"✅ Verified object removal")
        
        return True
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False


def main():
    """Run all MinIO tests."""
    print("MinIO Test Script")
    print("=" * 50)
    print(f"Endpoint: {MINIO_ENDPOINT}")
    print(f"Access Key: {MINIO_ACCESS_KEY}")
    print(f"Secure: {MINIO_SECURE}")
    print("=" * 50)
    print()
    
    # Create MinIO client
    client = get_minio_client()
    
    # Run tests
    tests = [
        ("Connection Test", lambda: test_connection(client)),
        ("Bucket Operations", lambda: test_bucket_operations(client)),
        ("Object Operations", lambda: test_object_operations(client)),
        ("Cleanup", lambda: test_cleanup(client)),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 30)
        success = test_func()
        results.append((test_name, success))
    
    # Summary
    print("\n" + "=" * 50)
    print("Test Summary:")
    print("=" * 50)
    
    all_passed = True
    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_name}: {status}")
        if not success:
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("✅ All tests passed!")
        return 0
    else:
        print("❌ Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())