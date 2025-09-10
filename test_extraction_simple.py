#!/usr/bin/env python3
"""
Simple PDF Extraction Test Script

This script provides an easy way to test the extraction functionality 
with any PDF file. It uses mock data that matches the requirements.

Usage:
  python test_extraction_simple.py [path_to_pdf]
  
If no path is provided, it will use a dummy path and show sample results.
"""

import os
import sys
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

try:
    from test_extraction_local import LocalExtractionTester
except ImportError as e:
    print(f"Error importing test module: {e}")
    print("Make sure test_extraction_local.py is in the same directory.")
    sys.exit(1)


def test_with_sample_pdf(pdf_path=None):
    """Test extraction with a PDF file or use sample data."""
    
    # Use provided path or create a dummy one for testing
    if pdf_path is None:
        pdf_path = "sample_invoice.pdf"
        print(f"No PDF path provided. Using sample path: {pdf_path}")
        print("Note: This will use mock data since the file doesn't exist.")
    
    print(f"\n🚀 Starting PDF extraction test...")
    print(f"PDF Path: {pdf_path}")
    
    # Initialize the tester
    tester = LocalExtractionTester()
    
    # Run the extraction test
    result = tester.test_pdf_extraction(pdf_path)
    
    # Print the formatted summary
    tester.print_analysis_summary(result)
    
    # Show some key data for verification
    if result.get('extraction_status') == 'completed':
        normalized = result.get('normalized_data', {})
        print(f"\n🔍 Quick Data Verification:")
        print(f"Raw data includes {len(normalized.get('labor', []))} workers:")
        
        for i, worker in enumerate(normalized.get('labor', [])[:3]):
            print(f"  {i+1}. {worker.get('name', 'Unknown')} - "
                  f"{worker.get('type', 'N/A')} - "
                  f"{worker.get('total_hours', 0)} hrs @ "
                  f"${worker.get('unit_price', 0):.2f}")
        
        if len(normalized.get('labor', [])) > 3:
            print(f"  ... and {len(normalized.get('labor', [])) - 3} more workers")
        
        return True
    else:
        print(f"\n❌ Test failed: {result.get('error', 'Unknown error')}")
        return False


def main():
    """Main function for simple testing."""
    
    # Get PDF path from command line or use default
    pdf_path = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Handle the case where no file is provided or file doesn't exist
    if pdf_path and not os.path.exists(pdf_path):
        print(f"⚠️  File not found: {pdf_path}")
        print("Proceeding with mock data for demonstration...")
    
    # Run the test
    success = test_with_sample_pdf(pdf_path)
    
    if success:
        print(f"\n✅ Extraction test completed successfully!")
        print(f"\nℹ️  This test demonstrates the full extraction pipeline:")
        print(f"   • Document processing (Textract simulation)")
        print(f"   • Data normalization (Bedrock simulation)")
        print(f"   • MSA rate comparison and variance detection")
        print(f"   • Compliance analysis and reporting")
        
        print(f"\nℹ️  To test with a real PDF:")
        print(f"   python test_extraction_simple.py /path/to/your/invoice.pdf")
    else:
        print(f"\n❌ Extraction test failed.")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
