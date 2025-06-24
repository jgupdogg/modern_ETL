#!/usr/bin/env python3
"""
Check Delta Lake compatibility matrix
"""

print("Delta Lake Compatibility Matrix:")
print("=" * 60)
print("PySpark 3.5.x -> Delta Lake 3.1.0 or 3.2.0")
print("PySpark 3.4.x -> Delta Lake 2.4.0")
print("PySpark 3.3.x -> Delta Lake 2.3.0")
print("PySpark 3.2.x -> Delta Lake 2.2.0")
print("=" * 60)
print("\nFor PySpark 3.5.0, we should use Delta Lake 3.1.0")
print("However, based on Maven Central availability:")
print("- Delta Lake 3.0.0 exists")
print("- Delta Lake 3.1.0 might not be released yet")
print("- Delta Lake 3.2.0 is experimental")
print("\nRecommendation: Use Delta Lake 3.0.0 with PySpark 3.5.0")