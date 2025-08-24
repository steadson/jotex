import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.date_utils import convert_date

def test_convert_date():
    """Test function to verify date conversion works as expected."""
    test_cases = [
        ("2025-01-07", 7, "2025-07-01"),  # Your example: 7 in position 3 → YYYY-DD-MM
        ("2025-07-01", 7, "2025-07-01"),  # 7 in position 2 → YYYY-MM-DD
        ("2025-15-01", 1, "2025-01-15"),  # 1 in position 3 → YYYY-DD-MM
        ("2025-01-15", 1, "2025-01-15"),  # 1 in position 2 → YYYY-MM-DD
        ("2025-25-12", 12, "2025-12-25"), # 12 in position 3 → YYYY-DD-MM
        ("2025-12-25", 12, "2025-12-25"), # 12 in position 2 → YYYY-MM-DD
        ("2025-20-03", 3, "2025-03-20"),  # 3 in position 3 → YYYY-DD-MM
        ("2025-03-20", 3, "2025-03-20"),  # 3 in position 2 → YYYY-MM-DD
        ("2025-05-08", 5, "2025-05-08"),  # 5 in position 2 → YYYY-MM-DD
        ("2025-08-05", 5, "2025-05-08"),  # 5 in position 3 → YYYY-DD-MM
        # XX/XX/YYYY format tests
        ("12/01/2025", 12, "2025-12-01"), # 12 in position 1 → MM/DD/YYYY
        ("01/12/2025", 12, "2025-12-01"), # 12 in position 2 → DD/MM/YYYY
        ("05/15/2025", 5, "2025-05-15"),  # 5 in position 1 → MM/DD/YYYY
        ("15/05/2025", 5, "2025-05-15"),  # 5 in position 2 → DD/MM/YYYY
        ("03/08/2025", 3, "2025-03-08"),  # 3 in position 1 → MM/DD/YYYY
        ("08/03/2025", 3, "2025-03-08"),  # 3 in position 2 → DD/MM/YYYY
    ]
    
    print("Testing convert_date function:")
    print("-" * 70)
    passed = 0
    failed = 0
    
    for input_date, month_val, expected in test_cases:
        result = convert_date(input_date, month_value=month_val)
        status = "✓ PASS" if result == expected else "✗ FAIL"
        print(f"{status} | Input: {input_date} (month={month_val}) → Output: {result} | Expected: {expected}")
        
        if result == expected:
            passed += 1
        else:
            failed += 1
    
    print("-" * 70)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("Test completed!")
    
    return failed == 0

def test_edge_cases():
    """Test edge cases and error handling."""
    print("\nTesting edge cases:")
    print("-" * 70)
    
    edge_cases = [
        ("", None, ""),                    # Empty string
        (None, 7, ""),                     # None input
        ("invalid-date", 7, ""),           # Invalid format
        ("2025-02-30", 2, ""),             # Invalid date (Feb 30)
        ("2025-13-15", 13, ""),            # Invalid month > 12
        ("25/12/2025", 12, "2025-12-25"),  # DD/MM/YYYY format
        ("2025-07-01 MY (UTC+08:00)", 7, "2025-07-01"),  # With UTC suffix
        ("31/01/2025", 1, "2025-01-31"),   # DD/MM/YYYY with day > 12
        ("01/31/2025", 1, "2025-01-31"),   # MM/DD/YYYY with day > 12
    ]
    
    passed = 0
    failed = 0
    
    for input_date, month_val, expected in edge_cases:
        result = convert_date(input_date, month_value=month_val)
        status = "✓ PASS" if result == expected else "✗ FAIL"
        print(f"{status} | Input: {repr(input_date)} (month={month_val}) → Output: {repr(result)} | Expected: {repr(expected)}")
        
        if result == expected:
            passed += 1
        else:
            failed += 1
    
    print("-" * 70)
    print(f"Edge Case Results: {passed} passed, {failed} failed")
    return failed == 0

def run_all_tests():
    """Run all date conversion tests."""
    print("=" * 70)
    print("DATE CONVERSION TESTS")
    print("=" * 70)
    
    test1_passed = test_convert_date()
    test2_passed = test_edge_cases()
    
    print("\n" + "=" * 70)
    if test1_passed and test2_passed:
        print("🎉 ALL TESTS PASSED!")
    else:
        print("❌ SOME TESTS FAILED!")
    print("=" * 70)
    
    return test1_passed and test2_passed

if __name__ == "__main__":
    run_all_tests() 