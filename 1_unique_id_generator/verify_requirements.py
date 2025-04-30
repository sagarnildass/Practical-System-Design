import time
import concurrent.futures
from collections import defaultdict
from snowflake_id_generator import SnowflakeIDGenerator


def test_uniqueness(num_ids=10000):
    """Test that generated IDs are unique.

    Args:
        num_ids (int): Number of IDs to generate

    Returns:
        bool: True if all IDs are unique, False otherwise
    """
    print(f"Testing uniqueness of {num_ids} IDs...")
    generator = SnowflakeIDGenerator(datacenter_id=1, machine_id=1)

    # Generate IDs
    ids = set()
    for _ in range(num_ids):
        id_val = generator.next_id()
        if id_val in ids:
            print(f"FAILURE: Duplicate ID found: {id_val}")
            return False
        ids.add(id_val)

    print(f"SUCCESS: All {num_ids} IDs are unique")
    return True


def test_numerical_values():
    """Test that IDs are numerical values only.

    Returns:
        bool: True if all IDs are numerical, False otherwise
    """
    print("Testing that IDs are numerical values only...")
    generator = SnowflakeIDGenerator(datacenter_id=1, machine_id=1)

    # Generate some IDs
    for _ in range(100):
        id_val = generator.next_id()
        if not isinstance(id_val, (int, float)):
            print(f"FAILURE: Non-numerical ID found: {id_val}")
            return False

    print("SUCCESS: All IDs are numerical values")
    return True


def test_64bit_constraint():
    """Test that IDs fit into 64 bits.

    Returns:
        bool: True if all IDs fit into 64 bits, False otherwise
    """
    print("Testing that IDs fit into 64 bits...")
    generator = SnowflakeIDGenerator(datacenter_id=1, machine_id=1)

    # Generate some IDs
    for _ in range(100):
        id_val = generator.next_id()
        # Check if ID fits into 64 bits (2^64 - 1)
        if id_val > 18446744073709551615 or id_val < 0:
            print(f"FAILURE: ID doesn't fit into 64 bits: {id_val}")
            return False

    print("SUCCESS: All IDs fit into 64 bits")
    return True


def test_time_ordering():
    """Test that IDs are ordered by time.

    Returns:
        bool: True if IDs are ordered by time, False otherwise
    """
    print("Testing that IDs are ordered by time...")
    generator = SnowflakeIDGenerator(datacenter_id=1, machine_id=1)

    # Generate IDs with time delays
    id1 = generator.next_id()
    time.sleep(1)
    id2 = generator.next_id()
    time.sleep(1)
    id3 = generator.next_id()

    if not (id1 < id2 < id3):
        print(f"FAILURE: IDs are not ordered by time: {id1}, {id2}, {id3}")
        return False

    print("SUCCESS: IDs are ordered by time")
    return True


def test_generation_rate(target_rate=10000, duration=1):
    """Test that the system can generate IDs at the required rate.

    Args:
        target_rate (int): Target IDs per second
        duration (int): Test duration in seconds

    Returns:
        bool: True if the system meets the rate requirement, False otherwise
    """
    print(f"Testing generation rate (target: {target_rate} IDs/sec)...")
    generators = [
        SnowflakeIDGenerator(datacenter_id=i % 2, machine_id=i % 16) for i in range(8)
    ]

    generated_ids = set()
    start_time = time.time()
    end_time = start_time + duration
    count = 0

    def generate_batch(gen, batch_size):
        batch_ids = set()
        for _ in range(batch_size):
            batch_ids.add(gen.next_id())
        return batch_ids

    # Use ThreadPoolExecutor to parallelize ID generation
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        while time.time() < end_time:
            batch_size = max(1, target_rate // 100)  # Generate in small batches
            futures = [
                executor.submit(generate_batch, gen, batch_size) for gen in generators
            ]

            for future in concurrent.futures.as_completed(futures):
                ids = future.result()
                count += len(ids)
                # Check for duplicates
                overlap = generated_ids.intersection(ids)
                if overlap:
                    print(f"FAILURE: Duplicate IDs found: {overlap}")
                    return False
                generated_ids.update(ids)

    actual_duration = time.time() - start_time
    rate = count / actual_duration

    print(f"Generated {count} unique IDs in {actual_duration:.2f} seconds")
    print(f"Rate: {rate:.2f} IDs/sec")

    if rate < target_rate:
        print(
            f"FAILURE: Generation rate {rate:.2f} IDs/sec is below target {target_rate} IDs/sec"
        )
        return False

    print(
        f"SUCCESS: Generation rate of {rate:.2f} IDs/sec exceeds target {target_rate} IDs/sec"
    )
    return True


def run_all_tests():
    """Run all tests to verify the implementation meets requirements."""
    print("===== VERIFYING SNOWFLAKE ID GENERATOR REQUIREMENTS =====\n")

    # Test 1: IDs must be unique
    if not test_uniqueness():
        return False
    print()

    # Test 2: IDs are numerical values only
    if not test_numerical_values():
        return False
    print()

    # Test 3: IDs fit into 64-bit
    if not test_64bit_constraint():
        return False
    print()

    # Test 4: IDs are ordered by date
    if not test_time_ordering():
        return False
    print()

    # Test 5: Ability to generate over 10,000 unique IDs per second
    if not test_generation_rate(target_rate=10000):
        return False
    print()

    print("===== ALL REQUIREMENTS VERIFIED SUCCESSFULLY =====")
    return True


if __name__ == "__main__":
    run_all_tests()
