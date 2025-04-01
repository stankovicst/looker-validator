"""Diagnostic script for Looker SDK"""
import os
import sys
import inspect
import looker_sdk

def run_diagnostic():
    print("\n===== LOOKER SDK DIAGNOSTIC =====")
    
    # Print SDK version
    print(f"\nLooker SDK Version:")
    try:
        print(f"  Version: {looker_sdk.__version__}")
    except AttributeError:
        print("  Could not determine SDK version")
    
    # Print available methods/attributes
    print("\nAvailable SDK attributes:")
    sdk_attrs = [attr for attr in dir(looker_sdk) if not attr.startswith('_')]
    print(f"  {', '.join(sdk_attrs)}")
    
    print("\n===== END DIAGNOSTIC =====\n")

run_diagnostic()