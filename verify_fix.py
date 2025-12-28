import sys
from unittest.mock import MagicMock, patch

# Mock dependencies before importing app.py
sys.modules['flask'] = MagicMock()
sys.modules['dotenv'] = MagicMock()
sys.modules['redis'] = MagicMock()
sys.modules['requests'] = MagicMock()
sys.modules['mysql-connector-python'] = MagicMock()

# Mock SQLAlchemy
mock_sqlalchemy = MagicMock()
sys.modules['sqlalchemy'] = mock_sqlalchemy
sys.modules['sqlalchemy.pool'] = MagicMock()

import app

def test_engine_caching():
    print("Testing SQLAlchemy engine caching...")
    
    # Reset mock and ENGINES
    mock_sqlalchemy.create_engine.reset_mock()
    app.ENGINES = {}
    
    brand = "TEST_BRAND"
    conn_str = "mysql+mysqlconnector://user:pass@host/db"
    date_str = "2023-12-23"
    
    # Mock get_conn_str_for_brand to return the same conn_str
    with patch('app.get_conn_str_for_brand', return_value=conn_str):
        # First call
        app.fetch_metrics_for_brand(brand, date_str)
        assert mock_sqlalchemy.create_engine.call_count == 1
        print("First call: Engine created.")
        
        # Second call
        app.fetch_metrics_for_brand(brand, date_str)
        assert mock_sqlalchemy.create_engine.call_count == 1
        print("Second call: Engine reused from cache.")
        
        # Verify pool arguments and SSL config
        args, kwargs = mock_sqlalchemy.create_engine.call_args
        assert kwargs['poolclass'] is not None
        
        # Verify SSL args
        connect_args = kwargs.get('connect_args', {})
        assert connect_args.get('ssl_verify_cert') is False
        assert connect_args.get('ssl_verify_identity') is False
        print("Correct SSL settings verified.")

if __name__ == "__main__":
    try:
        test_engine_caching()
        print("\nVerification SUCCESSFUL.")
    except Exception as e:
        print(f"\nVerification FAILED: {e}")
        sys.exit(1)
