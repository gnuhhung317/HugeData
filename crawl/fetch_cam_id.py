import re
import json
import sys

def extract_camera_ids(text):
    # Regex to find "Value":" followed by 24 hex characters
    pattern = r'"Value":"([a-fA-F0-9]{24})"'
    matches = re.findall(pattern, text)
    # Remove duplicates by converting to set and back to list
    unique_ids = list(set(matches))
    return unique_ids

def main():
    if len(sys.argv) != 2:
        print("Usage: python fetch_cam_id.py <input_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            text = f.read()
    except FileNotFoundError:
        print(f"File {input_file} not found.")
        sys.exit(1)

    ids = extract_camera_ids(text)
    output_file = 'camera_ids.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(ids, f, indent=4)

    print(f"Extracted {len(ids)} IDs and saved to {output_file}")

if __name__ == "__main__":
    main()
