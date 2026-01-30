import unittest
import pandas as pd
import os
import shutil
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch
from ib_downloader.downloader import get_storage_dir, resample_candles, load_parquet_files, DownloadOptions, \
    download_historical_data, download_daily_data


class TestDownloader(unittest.TestCase):
    def test_get_storage_dir(self):
        base_path = "data"
        ticker = "AAPL"
        year = "2026"
        month = "01"
        expected = os.path.join(base_path, ticker, year, month)
        self.assertEqual(get_storage_dir(base_path, ticker, year, month), expected)

    def test_resample_candles(self):
        # Create dummy minute data
        index = pd.date_range("2026-01-01 09:30:00", periods=30, freq="min")
        data = {
            "open": [100.0] * 30,
            "high": [105.0] * 30,
            "low": [95.0] * 30,
            "close": [101.0] * 30,
            "volume": [100] * 30
        }
        df = pd.DataFrame(data, index=index)
        
        # Test 15min resampling
        resampled_15 = resample_candles(df, "15min")
        self.assertEqual(len(resampled_15), 2)
        self.assertEqual(resampled_15.iloc[0]["open"], 100.0)
        self.assertEqual(resampled_15.iloc[0]["high"], 105.0)
        self.assertEqual(resampled_15.iloc[0]["volume"], 1500)

    def test_load_parquet_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a dummy parquet file
            df = pd.DataFrame({
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000]
            }, index=[pd.Timestamp("2026-01-01 09:30:00")])
            
            # filename format: YYYY-MM-DD.parquet.snappy
            filename = "2026-01-01.parquet.snappy"
            file_path = os.path.join(tmpdir, filename)
            df.to_parquet(file_path, compression="snappy")
            
            loaded_df = load_parquet_files(tmpdir)
            self.assertFalse(loaded_df.empty)
            self.assertEqual(len(loaded_df), 1)
            self.assertEqual(loaded_df.iloc[0]["open"], 100.0)

    @patch("ibapi.client.EClient.connect")
    @patch("ibapi.client.EClient.disconnect")
    @patch("ibapi.client.EClient.isConnected")
    @patch("ibapi.client.EClient.run")
    @patch("ib_downloader.downloader.time.sleep")
    def test_download_historical_data(self, mock_sleep, mock_run, mock_is_connected, mock_disconnect, mock_connect):
        # Setup mock behavior
        mock_is_connected.return_value = True
        
        from ibapi.common import BarData
        
        # We patch reqHistoricalData on the class so we can access 'self' (the IBapi instance)
        # We must use autospec=False or just patch it normally to ensure it's treated as a method
        with patch("ib_downloader.downloader.IBapi.reqHistoricalData", autospec=True) as mock_req_hist:
            def side_effect_with_self(app_instance, reqId, contract, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate, keepUpToDate, chartOptions):
                # bar.date format: "%Y%m%d %H:%M:%S" + " US/Eastern" (based on downloader.py)
                bar = BarData()
                bar.date = "20260101 12:00:00 US/Eastern"
                bar.open = 100.0
                bar.high = 110.0
                bar.low = 90.0
                bar.close = 105.0
                bar.volume = 1000
                
                app_instance.historicalData(reqId, bar)
                app_instance.historicalDataEnd(reqId, "", "")
            
            mock_req_hist.side_effect = side_effect_with_self
            
            # Options
            with tempfile.TemporaryDirectory() as tmpdir:
                cfg = DownloadOptions(
                    ticker="AAPL",
                    start_date="2026-01-01",
                    end_date="2026-01-01",
                    storage_dir=tmpdir
                )
                
                download_historical_data(cfg)
                
                # Verify that parquet file was created
                expected_path = os.path.join(tmpdir, "AAPL", "2026", "01", "2026-01-01.parquet.snappy")
                self.assertTrue(os.path.exists(expected_path), f"File not found at {expected_path}")
                
                # Load and check data
                df = pd.read_parquet(expected_path)
                self.assertEqual(len(df), 1)
                self.assertEqual(df.iloc[0]["close"], 105.0)

    def test_download_historical_data_day(self):
        # Setup mock behavior
        tmpdir = "D:/Projekte/Tradez/github/MarkoViz/data"
        cfg = DownloadOptions(
            ticker="AAPL",
            start_date="2026-01-12",
            end_date="2026-01-14",
            storage_dir=tmpdir
        )

        download_historical_data(cfg)

        # Verify that parquet file was created
        expected_path = os.path.join(tmpdir, "AAPL", "2026", "01", "2026-01-12.parquet")
        self.assertTrue(os.path.exists(expected_path), f"File not found at {expected_path}")

        # Load and check data
        df = pd.read_parquet(expected_path)
        self.assertEqual(len(df), 16*60) # 16 stunden Handelszeit
        #self.assertEqual(df.iloc[0]["close"], 105.0)

    def test_download_historical_data_daily(self):
        # Setup mock behavior
        tmpdir = "D:/Projekte/Tradez/github/MarkoViz/data"

        cfg = DownloadOptions(
            ticker="AAPL",
            start_date="2025-01-01",
            end_date="2025-12-31",
            storage_dir=tmpdir,
            durationStr='1 Y',
            barSizeSetting='1 D',
        )

        download_daily_data(cfg)

        # Verify that parquet file was created
        expected_path = os.path.join(tmpdir, "daily", "AAPL.parquet")
        self.assertTrue(os.path.exists(expected_path), f"File not found at {expected_path}")

        # Load and check data
        df = pd.read_parquet(expected_path)
        self.assertEqual(len(df), 250) # ungefähr
        #self.assertEqual(df.iloc[0]["close"], 105.0)

if __name__ == "__main__":
    unittest.main()
