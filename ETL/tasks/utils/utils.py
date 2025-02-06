import re


def sanitize_filename(filename):
    # Replace unsupported characters with an underscore
    return re.sub(r'[^a-zA-Z0-9._-]', '_', filename)
