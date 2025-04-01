"""
Comprehensive diagnostic script for Looker SDK.
Run this with: python looker_sdk_diagnostic.py
"""

import os
import sys
import looker_sdk
import importlib
import pkg_resources

def run_diagnostic():
    """Run comprehensive diagnostics on the installed Looker SDK"""
    print("\n===== LOOKER SDK DIAGNOSTIC =====")
    
    # Print Python version
    print(f"\nPython Version: {sys.version}")
    
    # Print SDK version
    print(f"\nLooker SDK Package Version:")
    try:
        sdk_version = pkg_resources.get_distribution("looker-sdk").version
        print(f"  Version: {sdk_version}")
    except Exception as e:
        print(f"  Could not determine SDK version: {str(e)}")
    
    # Print SDK module structure
    print("\nLooker SDK Module Structure:")
    for root, dirs, files in os.walk(os.path.dirname(looker_sdk.__file__)):
        rel_path = os.path.relpath(root, os.path.dirname(looker_sdk.__file__))
        if rel_path == ".":
            rel_path = ""
        print(f"  {rel_path}/")
        for file in files:
            if file.endswith(".py") and not file.startswith("__"):
                print(f"    - {file}")
    
    # Print available methods/attributes
    print("\nAvailable SDK Attributes:")
    sdk_attrs = [attr for attr in dir(looker_sdk) if not attr.startswith('_')]
    print(f"  {', '.join(sdk_attrs)}")
    
    # Try to find available models
    print("\nLooking for models modules:")
    try:
        # Try to find models modules at different paths
        for path in [
            "looker_sdk.models",
            "looker_sdk.rtl.models",
            "looker_sdk.sdk.api31.models",
            "looker_sdk.sdk.api40.models",
        ]:
            try:
                models = importlib.import_module(path)
                print(f"  ✓ Found models at: {path}")
                
                # List a few model classes
                model_classes = [name for name in dir(models) if not name.startswith("_") and name[0].isupper()]
                if model_classes:
                    print(f"    Sample model classes: {', '.join(model_classes[:5])}")
            except ImportError:
                print(f"  ✗ No models found at: {path}")
    except Exception as e:
        print(f"  Error checking for models: {str(e)}")
    
    # Try SDK initialization
    print("\nTrying SDK Initialization:")
    
    # Set environment variables for testing
    os.environ["LOOKERSDK_BASE_URL"] = "https://example.looker.com"
    os.environ["LOOKERSDK_CLIENT_ID"] = "test_id"
    os.environ["LOOKERSDK_CLIENT_SECRET"] = "test_secret"
    
    # Try different initialization methods
    for method_name, args in [
        ("init", []), 
        ("init31", []), 
        ("init40", []),
        ("LookerSDK", []),
        ("get_client", ["3.1"]),
        ("get_client", ["4.0"]),
    ]:
        if hasattr(looker_sdk, method_name):
            try:
                print(f"  Trying looker_sdk.{method_name}({', '.join(map(repr, args))})...")
                method = getattr(looker_sdk, method_name)
                # Call the method with the specified arguments
                sdk_instance = method(*args)
                print(f"  ✓ {method_name}({', '.join(map(repr, args))}) succeeded")
                
                # Sample SDK methods
                sdk_methods = [m for m in dir(sdk_instance) if not m.startswith('_') and callable(getattr(sdk_instance, m))]
                if sdk_methods:
                    sample = ', '.join(sorted(sdk_methods)[:5])
                    print(f"    Sample available methods: {sample}...")
            except Exception as e:
                print(f"  ✗ {method_name}({', '.join(map(repr, args))}) failed: {str(e)}")
        else:
            print(f"  ✗ {method_name} is not available")
    
    # Output pip freeze for debugging
    print("\nInstalled Packages (pip freeze):")
    try:
        import subprocess
        result = subprocess.run([sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True)
        for line in result.stdout.splitlines():
            if "looker" in line.lower():
                print(f"  {line}")
    except Exception as e:
        print(f"  Error getting pip freeze: {str(e)}")
    
    print("\n===== END DIAGNOSTIC =====\n")

if __name__ == "__main__":
    run_diagnostic()