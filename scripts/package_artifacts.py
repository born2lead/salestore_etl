"""
Build and Package Deployment Artifacts for AWS DE Pipeline.

Creates:
- dist/pipeline_utils.zip: Bundles src/utils into a zip archive for AWS Glue --extra-py-files.
"""

import os
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DIST_DIR = PROJECT_ROOT / "dist"
SRC_UTILS_DIR = PROJECT_ROOT / "src" / "utils"

def package_glue_utils():
    """Package src/utils into dist/pipeline_utils.zip with 'utils' top-level directory structure."""
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = DIST_DIR / "pipeline_utils.zip"
    
    print(f"Packaging Glue utility module into {zip_path}...")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Create empty utils/__init__.py if not exists
        zipf.writestr("utils/__init__.py", "# Package marker for Glue utils\n")
        
        for py_file in SRC_UTILS_DIR.glob("*.py"):
            if py_file.name != "__init__.py":
                archive_name = f"utils/{py_file.name}"
                zipf.write(py_file, arcname=archive_name)
                print(f"  + Added {py_file.relative_to(PROJECT_ROOT)} -> {archive_name}")
                
    print(f"Successfully created {zip_path} ({zip_path.stat().st_size} bytes)\n")

if __name__ == "__main__":
    package_glue_utils()
