import time
import struct
from pathlib import Path
# Note: McapROS2Writer seems to be unavailable in the current library version.
# This script is for reference and will not run without a library that supports writing ROS2 MCAPs.
# from mcap_ros2.writer import McapROS2Writer

# Mock ROS2 message classes to simulate the structure
class MockHeader:
    def __init__(self, sec, nanosec):
        self.stamp = {'sec': sec, 'nanosec': nanosec}

class MockTwist:
    def __init__(self, x):
        self.linear = {'x': x}

class MockVelocityStatus:
    def __init__(self, x):
        self.twist = MockTwist(x)

class MockImuData:
    def __init__(self, sec, nanosec):
        self.header = MockHeader(sec, nanosec)

class MockCustomBinary:
    def __init__(self, data):
        self.data = data

def create_dummy_mcap(output_path: Path):
    """Generates a dummy MCAP file for testing purposes."""
    if output_path.exists():
        output_path.unlink()

    # This block will fail unless a suitable writer is available.
    # The import has been commented out to prevent errors.
    # with open(output_path, "wb") as f, McapROS2Writer(f) as writer:
    #     # 1. /vehicle/status/velocity (simplified geometry_msgs/msg/TwistStamped)
    #     for i in range(10):
    #         t = time.time_ns()
    #         msg = MockVelocityStatus(10.0 + i) # Start from 10 m/s
    #         writer.write_message(
    #             topic="/vehicle/status/velocity",
    #             schema_name="geometry_msgs/msg/TwistStamped",
    #             message=msg,
    #             log_time=t,
    #             publish_time=t,
    #         )
    #         time.sleep(0.01)

    #     # 2. /sensor/imu/data (simplified sensor_msgs/msg/Imu) - close to 100Hz
    #     start_time_ns = time.time_ns()
    #     for i in range(100):
    #         # Add some noise to a 10ms (100Hz) interval
    #         timestamp_ns = start_time_ns + i * 10_000_000 + (i % 5 - 2) * 500_000 # +/- 0.5ms noise
    #         sec = timestamp_ns // 1_000_000_000
    #         nanosec = timestamp_ns % 1_000_000_000
    #         msg = MockImuData(sec, nanosec)
    #         writer.write_message(
    #             topic="/sensor/imu/data",
    #             schema_name="sensor_msgs/msg/Imu",
    #             message=msg,
    #             log_time=timestamp_ns,
    #             publish_time=timestamp_ns,
    #         )
    #         time.sleep(0.01)

    #     # 3. /custom/binary_topic (simplified std_msgs/msg/ByteMultiArray)
    #     for i in range(5):
    #         t = time.time_ns()
    #         # 8 bytes of float64 data (little-endian)
    #         # value = 123.456 + i
    #         # '<d' is for little-endian double
    #         binary_data = b'\x00\x01\x02\x03\x04\x05\x06\x07' + struct.pack('<d', 123.456 + i)
    #         msg = MockCustomBinary(binary_data)
    #         writer.write_message(
    #             topic="/custom/binary_topic",
    #             schema_name="std_msgs/msg/ByteMultiArray",
    #             message=msg,
    #             log_time=t,
    #             publish_time=t,
    #         )
    #         time.sleep(0.01)

    # Since the writer is not available, create an empty file as a placeholder.
    with open(output_path, "wb") as f:
        pass # Creates an empty file

    print(f"Generated dummy MCAP file: {output_path}")

if __name__ == "__main__":
    create_dummy_mcap(Path("dummy.mcap"))
