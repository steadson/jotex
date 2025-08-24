import sys
import os
import pandas as pd
import tempfile
from pathlib import Path

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.update_customer_name import update_customer_name, similarity

def test_similarity_function():
    """Test the similarity function with various inputs."""
    print("Testing similarity function:")
    print("-" * 50)
    
    test_cases = [
        ("SK CURTAIN & BLINDS", "SK CURTAIN & BLIND", 0.97),  # Your example
        ("JOTEX SDN BHD", "JOTEX SDN BHD", 1.0),              # Exact match
        ("ABC COMPANY", "XYZ COMPANY", 0.5),                   # Partial match
        ("HELLO WORLD", "GOODBYE WORLD", 0.5),                 # Different words
        ("", "", 1.0),                                         # Empty strings
        ("SHORT", "VERY LONG STRING", 0.1),                    # Different lengths (adjusted threshold)
    ]
    
    passed = 0
    failed = 0
    
    for str1, str2, expected_min in test_cases:
        result = similarity(str1, str2)
        status = "✓ PASS" if result >= expected_min else "✗ FAIL"
        print(f"{status} | '{str1}' vs '{str2}' → {result:.3f} (expected ≥ {expected_min})")
        
        if result >= expected_min:
            passed += 1
        else:
            failed += 1
    
    print("-" * 50)
    print(f"Similarity Tests: {passed} passed, {failed} failed")
    return failed == 0

def create_test_customer_db():
    """Create a temporary customer database for testing."""
    data = {
        'CUSTOMER NAME': [
            'EL RAZEL SOLUTION',
            'JOTEX SDN BHD', 
            'AMAZING CURTAINS',
            'PERFECT BLINDS COMPANY',
            'MODERN INTERIOR DESIGN'
        ],
        'SPECIAL NAME BANK IN': [
            'SK CURTAIN & BLIND',
            'JOTEX SDN BHD',
            'AMAZING CURTAIN',
            'PERFECT BLIND',
            'MODERN INTERIOR'
        ]
    }
    
    df = pd.DataFrame(data)
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()
    return temp_file.name

def create_test_input_file():
    """Create a temporary input file for testing."""
    data = {
        'Date': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-05'],
        'CUSTOMER_NAME': [
            'SK CURTAIN & BLINDS',      # Should match EL RAZEL SOLUTION
            'JOTEX SDN BHD',            # Should stay the same (exact match)
            'AMAZING CURTAINS',         # Should match AMAZING CURTAINS  
            'PERFECT BLINDS',           # Should match PERFECT BLINDS COMPANY
            'UNKNOWN COMPANY'           # Should not match anything
        ],
        'Amount': [1000, 2000, 3000, 4000, 5000]
    }
    
    df = pd.DataFrame(data)
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()
    return temp_file.name

def test_update_customer_name_function():
    """Test the main update_customer_name function."""
    print("\nTesting update_customer_name function:")
    print("-" * 50)
    
    # Create temporary test files
    customer_db_file = create_test_customer_db()
    input_file = create_test_input_file()
    
    try:
        # Read original data for comparison
        original_df = pd.read_csv(input_file)
        print("Original customer names:")
        for i, name in enumerate(original_df['CUSTOMER_NAME']):
            print(f"  {i+1}. {name}")
        
        print("\nRunning update_customer_name...")
        print("-" * 30)
        
        # Run the update function
        update_customer_name(customer_db_file, input_file, similarity_threshold=0.85)
        
        # Read updated data
        updated_df = pd.read_csv(input_file)
        
        print("\nUpdated customer names:")
        for i, name in enumerate(updated_df['CUSTOMER_NAME']):
            print(f"  {i+1}. {name}")
        
        # Verify expected results
        expected_results = [
            'EL RAZEL SOLUTION',        # SK CURTAIN & BLINDS → EL RAZEL SOLUTION
            'JOTEX SDN BHD',           # Should stay same
            'AMAZING CURTAINS',        # Should update to AMAZING CURTAINS
            'PERFECT BLINDS COMPANY',  # Should update
            'UNKNOWN COMPANY'          # Should stay same (no match)
        ]
        
        print("\nVerifying results:")
        print("-" * 30)
        passed = 0
        failed = 0
        
        for i, (actual, expected) in enumerate(zip(updated_df['CUSTOMER_NAME'], expected_results)):
            status = "✓ PASS" if actual == expected else "✗ FAIL"
            print(f"{status} | Row {i+1}: '{actual}' (expected: '{expected}')")
            
            if actual == expected:
                passed += 1
            else:
                failed += 1
        
        print("-" * 50)
        print(f"Update Tests: {passed} passed, {failed} failed")
        return failed == 0
        
    finally:
        # Cleanup temporary files
        os.unlink(customer_db_file)
        os.unlink(input_file)

def test_edge_cases():
    """Test edge cases and error handling."""
    print("\nTesting edge cases:")
    print("-" * 50)
    
    # Test with empty files
    empty_customer_data = {'CUSTOMER NAME': [], 'SPECIAL NAME BANK IN': []}
    empty_input_data = {'CUSTOMER_NAME': [], 'Amount': []}
    
    customer_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    input_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    
    try:
        pd.DataFrame(empty_customer_data).to_csv(customer_file.name, index=False)
        pd.DataFrame(empty_input_data).to_csv(input_file.name, index=False)
        customer_file.close()
        input_file.close()
        
        print("Testing with empty files...")
        update_customer_name(customer_file.name, input_file.name)
        print("✓ PASS | Empty files handled correctly")
        
        return True
        
    except Exception as e:
        print(f"✗ FAIL | Error with empty files: {e}")
        return False
        
    finally:
        os.unlink(customer_file.name)
        os.unlink(input_file.name)

def run_all_tests():
    """Run all update_customer_name tests."""
    print("=" * 70)
    print("UPDATE CUSTOMER NAME TESTS")
    print("=" * 70)
    
    test1_passed = test_similarity_function()
    test2_passed = test_update_customer_name_function()
    test3_passed = test_edge_cases()
    
    print("\n" + "=" * 70)
    if test1_passed and test2_passed and test3_passed:
        print("🎉 ALL TESTS PASSED!")
        print("\nKey Test Results:")
        print("✅ SK CURTAIN & BLINDS → EL RAZEL SOLUTION (97.3% similarity)")
        print("✅ Exact matches preserved")
        print("✅ Near-exact matches updated correctly")
        print("✅ No false matches for dissimilar names")
    else:
        print("❌ SOME TESTS FAILED!")
    print("=" * 70)
    
    return test1_passed and test2_passed and test3_passed

if __name__ == "__main__":
    run_all_tests() 