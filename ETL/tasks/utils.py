import re


def sanitize_filename(file_path: str) -> str:
    # Extract the filename using regex (get text after the last "/")
    filename = re.search(r"[^/]+$", file_path)
    if filename:
        filename = filename.group(0)  # Get the matched filename
        # Sanitize the filename by replacing unsupported characters
        return re.sub(r'[^a-zA-Z0-9._-]', '_', filename).replace(".html", "")
    return ""
