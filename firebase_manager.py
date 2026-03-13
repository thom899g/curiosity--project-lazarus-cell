"""
Firebase State Manager
Handles all Firebase interactions with robust error handling and connection pooling.
"""
import logging
from typing import Dict, Any, Optional, List
import time
import hashlib
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import credentials, firestore, db
from firebase_admin.exceptions import FirebaseError

from lazaros_config import config

logger = logging.getLogger(__name__)

class FirebaseManager:
    """Manages Firebase connections and operations with automatic retry logic"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirebaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._app = None
            self._firestore = None
            self._realtime_db = None
            self._initialized = True
    
    def initialize(self) -> bool:
        """Initialize Firebase connection with exponential backoff"""
        if self._app is not None:
            logger.warning("Firebase already initialized")
            return True
            
        try:
            # Validate credentials exist
            if not config.firebase.validate():
                logger.error("Firebase configuration invalid")
                return False
            
            cred = credentials.Certificate(config.firebase.credentials_path)
            
            # Initialize with multiple retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self._app = firebase_admin.initialize_app(
                        cred,
                        {
                            'projectId': config.firebase.project_id,
                            'databaseURL': config.firebase.realtime_db_url
                        }
                    )
                    self._firestore = firestore.client(app=self._app)
                    
                    if config.firebase.realtime_db_url:
                        self._realtime_db = db.reference(
                            url=config.firebase.realtime_db_url,
                            app=self._app
                        )
                    
                    logger.info("Firebase initialized successfully")
                    return True
                    
                except FirebaseError as e:
                    logger.warning(f"Firebase init attempt {attempt + 1} failed: {e}")
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
        except Exception as e:
            logger.error(f"Critical Firebase initialization error: {e}")
            self._app = None
            return False
    
    @property
    def firestore(self) -> firestore.Client:
        """Get Firestore client with lazy initialization"""
        if self._firestore is None:
            if not self.initialize():
                raise RuntimeError("Firebase initialization failed")
        return self._firestore
    
    @property
    def realtime_db(self) -> Optional[db.Reference]:
        """Get Realtime Database reference"""
        if self._realtime_db is None and config.firebase.realtime_db_url:
            if not self.initialize():
                raise RuntimeError("Firebase initialization failed")
        return self._realtime_db
    
    def write_order_book(self, exchange: str, pair: str, data: Dict[str, Any]) -> bool:
        """Write order book snapshot to Firestore with checksum"""
        try:
            # Generate deterministic ID for deduplication
            timestamp = data.get('timestamp', int(time.time() * 1000))
            data_id = f"{exchange}_{pair}_{timestamp}"
            
            # Add metadata
            data['_id'] = data_id
            data['_timestamp'] = firestore.SERVER_TIMESTAMP
            data['_checksum'] = self._generate_checksum(data)
            
            # Write to Firestore
            doc_ref = self.firestore.collection(
                config.firebase.firestore_collections['order_books']
            ).document(data_id)
            
            doc_ref.set(data)
            
            logger.debug(f"Order book written: {data_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write order book: {e}")
            return False
    
    def write_decision(self, decision_data: Dict[str, Any]) -> str:
        """Write trading decision to Firestore with full audit trail"""
        try:
            # Generate experiment ID
            experiment_id = f"exp_{int(time.time() * 1000)}_{hashlib.md5(str(decision_data).encode()).hexdigest()[:8]}"
            
            # Add audit metadata
            decision_data['experiment_id'] = experiment_id
            decision_data['timestamp'] = firestore.SERVER_TIMESTAMP
            decision_data['_created_at'] = datetime.now(timezone.utc).isoformat()
            
            # Write to Firestore
            doc_ref = self.firestore.collection(
                config.firebase.firestore_collections['decisions']
            ).document(experiment_id)
            
            doc_ref.set(decision_data)
            logger.info(f"Decision logged: {experiment_id}")
            return experiment_id
            
        except Exception as e:
            logger.error(f"Failed to write decision: {e}")
            raise
    
    def write_execution(self, execution_data: Dict[str, Any]) -> str:
        """Write execution result to Firestore"""
        try:
            order_id = execution_data.get('order_id', f"order_{int(time.time() * 1000)}")
            
            execution_data['_timestamp'] = firestore.SERVER_TIMESTAMP
            execution_data['_logged_at'] = datetime.now(timezone.utc).isoformat()
            
            doc_ref = self.firestore.collection(
                config.firebase.firestore_collections['executions']
            ).document(order_id)
            
            doc_ref.set(execution_data)
            logger.info(f"Execution logged: {order_id}")
            return order_id
            
        except Exception as e:
            logger.error(f"Failed to write execution: {e}")
            raise
    
    def read_config(self, module: str, parameter: str) -> Optional[Any]:
        """Read configuration parameter from Firestore"""
        try:
            doc_ref = self.firestore.collection(
                config.firebase.firestore_collections['config']
            ).document(module).collection('parameters').document(parameter)
            
            doc = doc_ref.get()
            if doc.exists:
                return doc.to_dict().get('value')
            return None
            
        except Exception as e:
            logger.error(f"Failed to read config {module}.{parameter}: {e}")
            return None
    
    def update_config(self, module: str, parameter: str, value: Any) -> bool:
        """Update configuration parameter in Firestore"""
        try:
            doc_ref = self.firestore.collection(
                config.firebase.firestore_collections['config']
            ).document(module).collection('parameters').document(parameter)
            
            doc_ref.set({
                'value': value,
                'updated_at': firestore.SERVER_TIMESTAMP,
                'updated_by': 'system'
            })
            logger.info(f"Config updated: {module}.{parameter} = {value}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update config: {e}")
            return False
    
    def check_kill_switch(self) -> bool:
        """Check if kill switch is armed in Realtime Database"""
        try:
            if not self.realtime_db:
                logger.warning("Realtime DB not configured, kill switch disabled")
                return False
            
            kill_ref = self.realtime_db.child(config.kill_switch_path)
            status = kill_ref.get()
            
            if status == "ARMED":
                logger.critical("KILL SWITCH ARMED - System halted")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to check kill switch: {e}")
            return False  # Default to not