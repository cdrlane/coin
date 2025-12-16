"""
Test script to check pymannkendall result attributes
"""

try:
    import pymannkendall as mk
    
    # Create sample data
    data = [10, 12, 15, 14, 18, 20, 22, 21, 25, 28, 30]
    
    # Run test
    result = mk.original_test(data)
    
    print("Mann-Kendall Test Result Attributes:")
    print("=" * 60)
    print(f"Type: {type(result)}")
    print(f"\nAll attributes: {dir(result)}")
    print("\n" + "=" * 60)
    print("Accessible values:")
    print("=" * 60)
    
    # Try to access common attributes
    attrs_to_check = ['trend', 'h', 'p', 'z', 'tau', 'Tau', 's', 'var_s', 
                      'slope', 'Sen_slope', 'intercept']
    
    for attr in attrs_to_check:
        try:
            value = getattr(result, attr)
            print(f"{attr:<20} = {value}")
        except AttributeError:
            print(f"{attr:<20} = [NOT FOUND]")
    
    print("\n" + "=" * 60)
    print("Full result object:")
    print(result)
    
except ImportError:
    print("❌ pymannkendall not installed")
    print("   Install with: pip install pymannkendall")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()