#!/usr/bin/env python3
"""
fix_import_zip.py

Cleans up a Superset dashboard export ZIP file by:
1. Removing __MACOSX/ folders (Mac OS X resource forks)
2. Removing .DS_Store files
3. Flattening nested folder structure (e.g., superset_assets/ -> root)

Extracts to a clean directory for use with `superset import-directory`.
"""

import zipfile
import os
import sys
import shutil


def fix_superset_zip(input_path: str, output_dir: str) -> bool:
    """
    Read input ZIP, clean it, and extract to output directory.
    
    Args:
        input_path: Path to the original (polluted) ZIP file
        output_dir: Path to extract the cleaned files
    
    Returns:
        True if successful, False otherwise
    """
    if not os.path.exists(input_path):
        print(f"ERROR: Input file not found: {input_path}")
        return False

    # Remove existing output directory if it exists
    if os.path.exists(output_dir):
        print(f"Removing existing directory: {output_dir}")
        shutil.rmtree(output_dir)
    
    os.makedirs(output_dir, exist_ok=True)

    # Detect the nested root folder (e.g., "superset_assets/")
    nested_root = None
    
    try:
        with zipfile.ZipFile(input_path, 'r') as zin:
            namelist = zin.namelist()
            
            # Find the common nested root (first folder that contains metadata.yaml)
            for name in namelist:
                if name.endswith('metadata.yaml') and '/' in name:
                    # e.g., "superset_assets/metadata.yaml" -> "superset_assets/"
                    nested_root = name.rsplit('metadata.yaml', 1)[0]
                    break
            
            if not nested_root:
                print("WARNING: Could not detect nested root folder. Using files as-is.")
                nested_root = ""
            else:
                print(f"Detected nested root folder: '{nested_root}'")
            
            # Extract with cleaned structure
            for item in zin.infolist():
                name = item.filename
                
                # Skip __MACOSX folders
                if name.startswith('__MACOSX'):
                    print(f"  Skipping Mac artifact: {name}")
                    continue
                
                # Skip .DS_Store files
                if '.DS_Store' in name:
                    print(f"  Skipping .DS_Store: {name}")
                    continue
                
                # Skip empty directory entries that are just the nested root
                if name == nested_root:
                    continue
                
                # Strip the nested root prefix to flatten
                if nested_root and name.startswith(nested_root):
                    new_name = name[len(nested_root):]
                else:
                    new_name = name
                
                # Skip if nothing left after stripping
                if not new_name:
                    continue
                
                # Build output path
                output_path = os.path.join(output_dir, new_name)
                
                # Handle directories
                if name.endswith('/'):
                    os.makedirs(output_path, exist_ok=True)
                    print(f"  Created dir: {new_name}")
                else:
                    # Ensure parent directory exists
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    # Extract file
                    with zin.open(item) as src, open(output_path, 'wb') as dst:
                        dst.write(src.read())
                    print(f"  Extracted: {new_name}")
        
        print(f"\nSUCCESS: Cleaned files extracted to {output_dir}")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to process ZIP: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    # Default paths (inside the Superset container)
    input_zip = "/app/pythonpath/dashboards/superset_assets.zip"
    output_dir = "/app/pythonpath/dashboards/clean_assets"
    
    # Allow overriding via command line
    if len(sys.argv) >= 2:
        input_zip = sys.argv[1]
    if len(sys.argv) >= 3:
        output_dir = sys.argv[2]
    
    print(f"Fixing Superset dashboard ZIP...")
    print(f"  Input ZIP: {input_zip}")
    print(f"  Output Dir: {output_dir}")
    print()
    
    success = fix_superset_zip(input_zip, output_dir)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
