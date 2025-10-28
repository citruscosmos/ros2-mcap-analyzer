from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
import numpy as np
import struct
import re
from asteval import Interpreter
from natsort import natsorted
from tqdm import tqdm
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
import sys

class McapReader:
    """
    A class to read MCAP files once and process the data for multiple analysis tasks.
    """
    def __init__(self, analysis_tasks: List[Dict[str, Any]]):
        """Initializes the McapReader with a list of analysis task configurations."""
        self.task_processors = [TaskProcessor(task) for task in analysis_tasks]
        self.topic_to_processors = self._map_topics_to_processors()
        self.all_topics = list(self.topic_to_processors.keys())

    def _map_topics_to_processors(self) -> Dict[str, List['TaskProcessor']]:
        """Creates a mapping from topic names to the processors that use them."""
        mapping = {}
        for processor in self.task_processors:
            if processor.topic_name not in mapping:
                mapping[processor.topic_name] = []
            mapping[processor.topic_name].append(processor)
        return mapping

    def process_files(self, mcap_paths: List[Path]) -> Dict[str, pd.DataFrame]:
        """
        Processes multiple MCAP files, dispatching messages to the appropriate
        TaskProcessors, and returns a dictionary of DataFrames for each task.
        """
        # Initialize a dictionary to store lists of rows for each task
        task_results = {processor.task_id: [] for processor in self.task_processors}

        for mcap_path in tqdm(natsorted(mcap_paths), desc="Processing MCAP files"):
            try:
                with open(mcap_path, "rb") as f:
                    reader = make_reader(f, decoder_factories=[DecoderFactory()])
                    # Iterate over messages for all topics required by the tasks
                    for schema, channel, message, ros_msg in reader.iter_decoded_messages(topics=self.all_topics):
                        processors = self.topic_to_processors.get(channel.topic, [])
                        for processor in processors:
                            # Each processor processes the message
                            row = processor.process_message(message.log_time, ros_msg)
                            if row:
                                task_results[processor.task_id].append(row)
            except Exception as e:
                print(f"Error: An error occurred while processing MCAP file '{mcap_path}': {e}", file=sys.stderr)
                continue

        # Convert lists of rows into DataFrames
        final_dataframes = {
            task_id: pd.DataFrame(rows) for task_id, rows in task_results.items()
        }
        return final_dataframes


class TaskProcessor:
    """
    A class that processes a single ROS message based on a specific analysis task config.
    It extracts fields, applies parsing directives, and computes a final value.
    """

    def __init__(self, analysis_task: Dict[str, Any]):
        """Initializes the TaskProcessor with a specific analysis task configuration."""
        self.task_id = analysis_task['id']
        self.topic_name = analysis_task['topic_name']
        self.field_names = [name.strip() for name in analysis_task['field_names'].split(',')]
        self.parse_string = analysis_task['parse_string']
        # Remove directives from the expression string for evaluation
        self.expression = re.sub(r'\([^)]+\)', '', self.parse_string)

        self.aeval = Interpreter()
        self.directives = self._extract_directives()

    def _extract_directives(self) -> Dict[str, str]:
        """
        Extracts the parsing method for each field from the parse_string.
        If a field is not in the 'field(...)' format, it defaults to 'default'.
        """
        directives = {}
        # Find all potential field names in the expression
        potential_fields = set(re.findall(r'([a-zA-Z_][\w\.]*)', self.parse_string))

        for field in self.field_names:
            # Only process fields that are listed in the 'field_names' config
            if field in potential_fields:
                match = re.search(re.escape(field) + r'\((.*?)\)', self.parse_string)
                if match:
                    directives[field] = match.group(1)
                else:
                    # If no directive is specified, use 'default'
                    directives[field] = 'default'
        return directives

    def _get_field_value(self, msg: Any, field_name: str) -> Any:
        """Retrieves a value from a nested message object using a dot-separated field name."""
        try:
            value = msg
            for attr in field_name.split('.'):
                if '[' in attr and attr.endswith(']'):
                    attr_name, index = attr[:-1].split('[')
                    value = getattr(value, attr_name)[int(index)]
                else:
                    value = getattr(value, attr)
            return value
        except (AttributeError, IndexError, KeyError) as e:
            print(f"Warning: Failed to retrieve value for field '{field_name}': {e}", file=sys.stderr)
            return None

    def _apply_directive(self, raw_value: Any, directive: str) -> Any:
        """Parses/converts a raw value according to the extracted directive."""
        if raw_value is None:
            return None

        if directive == 'default':
            return raw_value

        match = re.fullmatch(r'type:(\w+)', directive)
        if match:
            type_name = match.group(1)
            return pd.Series([raw_value]).astype(type_name).iloc[0]

        match = re.fullmatch(r'byte:(\d+)-(\d+)(?:,type:(\w+))?', directive)
        if match:
            start, length, type_name = match.groups()
            start, length = int(start), int(length)

            if not isinstance(raw_value, (bytes, list, np.ndarray)):
                 print(f"Warning: The 'byte' directive can only be applied to bytes or list/array types, but got {type(raw_value)}.", file=sys.stderr)
                 return None

            byte_slice = bytes(raw_value[start : start + length])

            if len(byte_slice) < length:
                print(f"Warning: Byte slice is shorter ({len(byte_slice)}) than the requested length ({length}).", file=sys.stderr)
                return None

            if type_name:
                format_map = {
                    'float64': '<d', 'float32': '<f', 'int64': '<q', 'uint64': '<Q',
                    'int32': '<i', 'uint32': '<I', 'int16': '<h', 'uint16': '<H',
                    'int8': '<b', 'uint8': '<B',
                }
                if type_name not in format_map:
                    raise ValueError(f"Unsupported type '{type_name}' specified in 'byte' directive.")
                return struct.unpack(format_map[type_name], byte_slice)[0]
            else:
                return byte_slice

        raise ValueError(f"Unknown parsing directive: '{directive}'")

    def process_message(self, mcap_timestamp_ns: int, ros_msg: Any) -> Dict[str, Any]:
        """
        Processes a single ROS message and returns a dictionary representing a single row of data.
        Returns None if the message cannot be processed.
        """
        raw_values = {}
        parsed_values_for_eval = {}

        for field in self.field_names:
            raw_val = self._get_field_value(ros_msg, field)
            raw_values[f"raw_{field}"] = raw_val

            if raw_val is None:
                return None  # Skip message if any raw value is None

            directive = self.directives.get(field)
            if directive is None:
                print(f"Warning: Directive for field '{field}' not found. Skipping message.", file=sys.stderr)
                return None  # Skip message if directive is missing

            parsed_val = self._apply_directive(raw_val, directive)
            if parsed_val is None:
                return None  # Skip message if parsing fails

            # Replace dots with underscores for use in the asteval symbol table
            safe_field_name = field.replace('.', '_')
            parsed_values_for_eval[safe_field_name] = parsed_val

        # Replace field names in the expression as well
        safe_expression = self.expression
        for field in self.field_names:
            safe_expression = safe_expression.replace(field, field.replace('.', '_'))

        self.aeval.symtable = parsed_values_for_eval
        final_value = self.aeval.eval(safe_expression)

        row = {"mcap_timestamp_ns": mcap_timestamp_ns}
        row.update(raw_values)
        row["parsed_value"] = final_value
        return row
