"""
Project Lazarus Core Configuration
Centralized configuration management with environment variable fallbacks.
Type hints and validation ensure runtime safety.
"""
import os
import json
from dataclasses import dataclass
from typing import Dict, List, Optional
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

@dataclass
class ExchangeConfig:
    """Exchange-specific configuration"""
    name: str
    api_key: Optional[str]
    api_secret: Optional[str]
    testnet: bool
    enabled: bool
    rate_limit_per_second: int
    supported_pairs: List[str]

@dataclass
class FirebaseConfig:
    """Firebase configuration with validation"""
    project_id: str
    credentials_path: str
    firestore_collections: Dict[str, str]
    realtime_db_url: str
    
    def validate(self) -> bool:
        """Validate Firebase configuration"""
        if not os.path.exists(self.credentials_path):
            logging.error(f"Firebase credentials not found at {self.credentials_path}")
            return False
        if not self.project_id:
            logging.error("Firebase project_id is required")
            return False
        return True

@dataclass
class TradingConfig:
    """Trading constraints and parameters"""
    max_capital_usd: float = 5.0
    max_position_usd: float = 0.5
    min_trade_interval_seconds: int = 30
    max_loss_before_pause_usd: float = 2.5
    paper_trading: bool = True
    risk_free_rate: float = 0.02  # Annualized
    
    def validate(self) -> bool:
        """Validate trading parameters"""
        if self.max_position_usd > self.max_capital_usd:
            logging.error("Max position cannot exceed total capital")
            return False
        if self.max_capital_usd <= 0:
            logging.error("Capital must be positive")
            return False
        return True

class LazarusConfig:
    """Main configuration manager"""
    
    def __init__(self):
        # Firebase Configuration
        self.firebase = FirebaseConfig(
            project_id=os.getenv("FIREBASE_PROJECT_ID", "project-lazarus"),
            credentials_path=os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./firebase-credentials.json"),
            firestore_collections={
                "order_books": "order_books",
                "decisions": "decisions",
                "executions": "executions",
                "config": "config",
                "patterns": "patterns"
            },
            realtime_db_url=os.getenv("FIREBASE_REALTIME_URL", "")
        )
        
        # Exchange Configurations
        self.exchanges = {
            "binance": ExchangeConfig(
                name="binance",
                api_key=os.getenv("BINANCE_API_KEY", ""),
                api_secret=os.getenv("BINANCE_API_SECRET", ""),
                testnet=True,  # Start with testnet
                enabled=True,
                rate_limit_per_second=10,
                supported_pairs=["BTC/USDT", "ETH/USDT"]
            ),
            "bitfinex": ExchangeConfig(
                name="bitfinex",
                api_key=os.getenv("BITFINEX_API_KEY", ""),
                api_secret=os.getenv("BITFINEX_API_SECRET", ""),
                testnet=False,  # Bitfinex doesn't have testnet
                enabled=False,  # Disable initially
                rate_limit_per_second=10,
                supported_pairs=["BTC/USDT", "ETH/USDT"]
            )
        }
        
        # Trading Configuration
        self.trading = TradingConfig()
        
        # VPS/Infrastructure
        self.vps_location = os.getenv("VPS_LOCATION", "fra1")  # Frankfurt
        self.decision_cycle_ms = (200, 800)  # Random range
        self.data_retention_minutes = 60
        
        # Kill Switch Configuration
        self.kill_switch_path = "/kill_switch/status"
        
        # Logging Configuration
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        
    def validate_all(self) -> bool:
        """Validate entire configuration"""
        if not self.firebase.validate():
            return False
        if not self.trading.validate():
            return False
        if not any(ex.enabled for ex in self.exchanges.values()):
            logging.error("At least one exchange must be enabled")
            return False
        return True
    
    def get_exchange_config(self, name: str) -> Optional[ExchangeConfig]:
        """Safely get exchange configuration"""
        return self.exchanges.get(name.lower())
    
    def to_dict(self) -> Dict:
        """Serialize configuration for logging"""
        return {
            "firebase": {
                "project_id": self.firebase.project_id,
                "credentials_path": self.firebase.credentials_path
            },
            "trading": {
                "max_capital_usd": self.trading.max_capital_usd,
                "paper_trading": self.trading.paper_trading
            },
            "exchanges": [ex.name for ex in self.exchanges.values() if ex.enabled]
        }

# Global configuration instance
config = LazarusConfig()