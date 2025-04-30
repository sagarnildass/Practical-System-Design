import sys
from snowflake_id_generator import SnowflakeIDGenerator


def visualize_binary(snowflake_id):
    """Visualize a snowflake ID in binary format with color coding.

    Args:
        snowflake_id (int): The snowflake ID to visualize
    """
    binary = bin(snowflake_id)[2:].zfill(64)

    # Split the binary string into its components
    sign_bit = binary[0]
    timestamp_bits = binary[1:42]
    datacenter_bits = binary[42:47]
    machine_bits = binary[47:52]
    sequence_bits = binary[52:]

    # Print the binary representation with colors if supported
    if sys.stdout.isatty():  # Check if running in a terminal that supports colors
        print(f"\n=== Binary Representation of ID: {snowflake_id} ===\n")
        print("Sign bit (1)       :", sign_bit)
        print("Timestamp (41)     :", timestamp_bits)
        print("Datacenter ID (5)  :", datacenter_bits)
        print("Machine ID (5)     :", machine_bits)
        print("Sequence (12)      :", sequence_bits)
    else:
        print(f"\n=== Binary Representation of ID: {snowflake_id} ===\n")
        print(f"Sign bit       (1): {sign_bit}")
        print(f"Timestamp     (41): {timestamp_bits}")
        print(f"Datacenter ID  (5): {datacenter_bits}")
        print(f"Machine ID     (5): {machine_bits}")
        print(f"Sequence      (12): {sequence_bits}")

    # Print the decimal values
    timestamp = int(timestamp_bits, 2)
    datacenter_id = int(datacenter_bits, 2)
    machine_id = int(machine_bits, 2)
    sequence = int(sequence_bits, 2)

    print("\n=== Decimal Values ===\n")
    print(f"Sign bit       : {int(sign_bit, 2)}")
    print(f"Timestamp      : {timestamp}")
    print(f"Datacenter ID  : {datacenter_id}")
    print(f"Machine ID     : {machine_id}")
    print(f"Sequence       : {sequence}")

    # Print a visual representation of the bit allocation
    print("\n=== Visual Bit Allocation ===\n")
    print("MSB                                                                LSB")
    print("┌─┬─────────────────────────────────────┬─────┬─────┬─────────────┐")
    print("│0│           Timestamp (41)            │DC(5)│MC(5)│Sequence (12)│")
    print("└─┴─────────────────────────────────────┴─────┴─────┴─────────────┘")
    print(" ↑                   ↑                     ↑     ↑         ↑")
    print(" 63                  22                   17    12         0")

    # Parse the complete ID
    parsed = SnowflakeIDGenerator.parse_id(snowflake_id)
    print(f"\n=== Parsed ID ===\n")
    for key, value in parsed.items():
        print(f"{key.replace('_', ' ').title()}: {value}")


def main():
    """Main function to generate and visualize a snowflake ID."""
    if len(sys.argv) > 1:
        try:
            snowflake_id = int(sys.argv[1])
            visualize_binary(snowflake_id)
        except ValueError:
            print(f"Error: '{sys.argv[1]}' is not a valid integer ID")
            sys.exit(1)
    else:
        # Generate a new ID
        print("No ID provided. Generating a new ID...")
        generator = SnowflakeIDGenerator(datacenter_id=1, machine_id=1)
        snowflake_id = generator.next_id()
        print(f"Generated ID: {snowflake_id}")
        visualize_binary(snowflake_id)

        print("\n=== Usage ===")
        print("Run with an existing ID to visualize:")
        print(f"python {sys.argv[0]} <snowflake_id>")


if __name__ == "__main__":
    main()
