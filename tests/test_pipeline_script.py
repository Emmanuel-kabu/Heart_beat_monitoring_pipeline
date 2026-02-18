"""
Test Pipeline Script

A standalone script that simulates different types of heart rate data
and tests the pipeline components without requiring Kafka or PostgreSQL.
Useful for quick validation during development.
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.generator.heartbeat_generator import HeartbeatGenerator, CUSTOMER_PROFILES
from src.validation.validator import HeartbeatValidator
from src.models import HeartbeatReading, CustomerProfile


def print_header(title: str) -> None:
    """Print a formatted section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def test_data_generation():
    """Test 1: Verify data generator produces valid data."""
    print_header("TEST 1: Data Generation")

    generator = HeartbeatGenerator(anomaly_probability=0.05)
    batch = generator.generate_batch(batch_size=20)

    print(f"Generated {len(batch)} readings:")
    for i, reading in enumerate(batch, 1):
        status = "ANOMALY" if reading.heart_rate < 50 or reading.heart_rate > 150 else "NORMAL"
        print(f"  [{i:2d}] {reading.customer_id} | HR: {reading.heart_rate:3d} bpm | {status}")

    print(f"\nTotal generated: {generator.total_generated}")
    return True


def test_validation_pipeline():
    """Test 2: Verify validator correctly identifies anomalies."""
    print_header("TEST 2: Validation Pipeline")

    validator = HeartbeatValidator(low_threshold=50, high_threshold=150)

    # Test various scenarios
    test_cases = [
        HeartbeatReading("CUST-001", 72, "2026-01-01T12:00:00+00:00"),    # Normal
        HeartbeatReading("CUST-002", 42, "2026-01-01T12:00:01+00:00"),    # Low anomaly
        HeartbeatReading("CUST-003", 175, "2026-01-01T12:00:02+00:00"),   # High anomaly
        HeartbeatReading("CUST-004", 50, "2026-01-01T12:00:03+00:00"),    # Boundary (normal)
        HeartbeatReading("CUST-005", 150, "2026-01-01T12:00:04+00:00"),   # Boundary (normal)
        HeartbeatReading("CUST-006", 49, "2026-01-01T12:00:05+00:00"),    # Boundary (low)
        HeartbeatReading("CUST-007", 151, "2026-01-01T12:00:06+00:00"),   # Boundary (high)
    ]

    print("Validation Results:")
    for reading in test_cases:
        is_valid, enriched, error = validator.validate_and_enrich(reading)
        status = "VALID" if is_valid else "INVALID"
        anomaly = f"[{enriched.anomaly_type}]" if enriched.is_anomaly else "[OK]"
        print(f"  {status} | {reading.customer_id} | HR: {reading.heart_rate:3d} | {anomaly}")

    print(f"\nValidator Stats: {validator.stats}")
    return True


def test_invalid_data_handling():
    """Test 3: Verify validator rejects invalid data."""
    print_header("TEST 3: Invalid Data Handling")

    validator = HeartbeatValidator()

    invalid_readings = [
        HeartbeatReading("", 72, "2026-01-01T12:00:00+00:00"),             # Empty ID
        HeartbeatReading("INVALID-001", 72, "2026-01-01T12:00:00+00:00"),  # Bad format
        HeartbeatReading("CUST-001", 10, "2026-01-01T12:00:00+00:00"),     # HR too low
        HeartbeatReading("CUST-001", 350, "2026-01-01T12:00:00+00:00"),    # HR too high
        HeartbeatReading("CUST-001", 72, ""),                               # Missing timestamp
        HeartbeatReading("CUST-001", 72, "bad-timestamp"),                  # Invalid timestamp
    ]

    print("Invalid Data Test Results:")
    all_rejected = True
    for reading in invalid_readings:
        is_valid, _, error = validator.validate_and_enrich(reading)
        status = "REJECTED" if not is_valid else "ACCEPTED (BUG!)"
        if is_valid:
            all_rejected = False
        print(f"  {status} | ID: '{reading.customer_id}' | HR: {reading.heart_rate} | Error: {error}")

    print(f"\nAll invalid data rejected: {all_rejected}")
    return all_rejected


def test_serialization():
    """Test 4: Verify JSON serialization/deserialization."""
    print_header("TEST 4: Serialization Round-Trip")

    generator = HeartbeatGenerator()
    readings = generator.generate_batch(batch_size=5)

    print("Serialization Test:")
    all_passed = True
    for reading in readings:
        json_str = reading.to_json()
        restored = HeartbeatReading.from_json(json_str)

        passed = (
            restored.customer_id == reading.customer_id
            and restored.heart_rate == reading.heart_rate
            and restored.timestamp == reading.timestamp
        )
        if not passed:
            all_passed = False

        status = "PASS" if passed else "FAIL"
        print(f"  {status} | {reading.customer_id} | JSON length: {len(json_str)} bytes")

    print(f"\nAll serialization tests passed: {all_passed}")
    return all_passed


def test_high_volume():
    """Test 5: Test with high volume of data."""
    print_header("TEST 5: High Volume Test")

    import time

    generator = HeartbeatGenerator(anomaly_probability=0.05)
    validator = HeartbeatValidator(low_threshold=50, high_threshold=150)

    num_readings = 10000

    # Generate
    start = time.time()
    readings = generator.generate_batch(batch_size=num_readings)
    gen_time = time.time() - start

    # Validate
    start = time.time()
    valid, rejected = validator.validate_batch(readings)
    val_time = time.time() - start

    print(f"Generated {num_readings} readings in {gen_time:.3f}s ({num_readings/gen_time:.0f} readings/sec)")
    print(f"Validated {num_readings} readings in {val_time:.3f}s ({num_readings/val_time:.0f} readings/sec)")
    print(f"Valid: {len(valid)} | Rejected: {len(rejected)}")
    print(f"Anomalies: {sum(1 for r in valid if r.is_anomaly)}")
    print(f"Anomaly rate: {sum(1 for r in valid if r.is_anomaly)/len(valid)*100:.1f}%")

    return len(rejected) == 0


def main():
    """Run all pipeline tests."""
    print_header("HEARTBEAT PIPELINE TEST SUITE")
    print("Running tests without Kafka/PostgreSQL dependencies...\n")

    tests = [
        ("Data Generation", test_data_generation),
        ("Validation Pipeline", test_validation_pipeline),
        ("Invalid Data Handling", test_invalid_data_handling),
        ("Serialization", test_serialization),
        ("High Volume", test_high_volume),
    ]

    results = []
    for name, test_func in tests:
        try:
            passed = test_func()
            results.append((name, passed))
        except Exception as e:
            print(f"\n  ERROR: {e}")
            results.append((name, False))

    # Summary
    print_header("TEST SUMMARY")
    passed_count = 0
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        emoji = "+" if passed else "-"
        print(f"  [{emoji}] {name}: {status}")
        if passed:
            passed_count += 1

    print(f"\n  Results: {passed_count}/{len(results)} tests passed")

    return all(passed for _, passed in results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
