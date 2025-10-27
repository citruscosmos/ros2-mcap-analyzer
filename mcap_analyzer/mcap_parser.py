from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
import struct
import re
from asteval import Interpreter
from natsort import natsorted
from tqdm import tqdm
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
import sys

class McapParser:
    """A class to parse data from MCAP files and generate a DataFrame."""

    def __init__(self, analysis_task: Dict[str, Any]):
        self.task_id = analysis_task['id']
        self.topic_name = analysis_task['topic_name']
        self.field_names = [name.strip() for name in analysis_task['field_names'].split(',')]
        self.parse_string = analysis_task['parse_string']

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

    def process_mcap_files(self, mcap_paths: List[Path]) -> pd.DataFrame:
        """Processes multiple MCAP files and generates a DataFrame for analysis."""
        all_rows = []
        # Remove directives from the expression string for evaluation
        expression = re.sub(r'\([^)]+\)', '', self.parse_string)

        for mcap_path in tqdm(natsorted(mcap_paths)):
            try:
                with open(mcap_path, "rb") as f:
                    reader = make_reader(f, decoder_factories=[DecoderFactory()])
                    for schema, channel, message, ros_msg in reader.iter_decoded_messages(topics=[self.topic_name]):
                        # ros_msg = reader.deserialize(schema, message)

                        raw_values = {}
                        parsed_values_for_eval = {}
                        should_skip = False

                        for field in self.field_names:
                            raw_val = self._get_field_value(ros_msg, field)
                            raw_values[f"raw_{field}"] = raw_val

                            if raw_val is None:
                                should_skip = True
                                break

                            directive = self.directives.get(field)
                            if directive is None:
                                print(f"Warning: Directive for field '{field}' not found. Skipping.", file=sys.stderr)
                                should_skip = True
                                break

                            parsed_val = self._apply_directive(raw_val, directive)
                            if parsed_val is None:
                                should_skip = True
                                break

                            # Replace dots with underscores for use in the asteval symbol table
                            safe_field_name = field.replace('.', '_')
                            parsed_values_for_eval[safe_field_name] = parsed_val

                        if should_skip:
                            continue

                        # Replace field names in the expression as well
                        safe_expression = expression
                        for field in self.field_names:
                           safe_expression = safe_expression.replace(field, field.replace('.', '_'))

                        self.aeval.symtable = parsed_values_for_eval
                        final_value = self.aeval.eval(safe_expression)

                        row = {"mcap_timestamp_ns": message.log_time}
                        row.update(raw_values)
                        row["parsed_value"] = final_value
                        all_rows.append(row)

            except Exception as e:
                print(f"Error: An error occurred while processing MCAP file '{mcap_path}': {e}", file=sys.stderr)
                continue

        return pd.DataFrame(all_rows)
