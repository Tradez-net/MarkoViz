from enum import IntEnum

class AssetCategory(IntEnum):
    STK = 0     # Stocks
    OPT = 1     # Options
    FOP = 2     # Futures Options
    CFD = 3     # Contracts for Difference
    FUT = 4     # Futures
    CASH = 5    # Cash Positions
    FXCFD = 6   # Forex CFDs
    BOND = 7    # Bonds

    @classmethod
    def from_str(cls, value: str):
        """Konvertiert den IB-String in eine AssetCategory-Enum."""
        if not value:
            return None
        mapping = {
            "STK": cls.STK,
            "OPT": cls.OPT,
            "FOP": cls.FOP,
            "CFD": cls.CFD,
            "FUT": cls.FUT,
            "CASH": cls.CASH,
            "FXCFD": cls.FXCFD,
            "BOND": cls.BOND,
        }
        return mapping.get(value.strip().upper())

    def __str__(self):
        """String-Repräsentation wie bei IB."""
        return list(self.__class__.__members__.keys())[self.value]
