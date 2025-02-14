import pandas as pd
import logging
import uuid
from abc import ABC, abstractmethod
from typing import Optional
from tqdm import tqdm
from cryptography import fernet as FR
from dotenv import load_dotenv
from Crypto.Cipher import AES
from Crypto.Hash import SHA256
from Crypto.Util.Padding import pad, unpad
import base64
import os
import hashlib

load_dotenv()
env_key=os.getenv('ENCRYPT_KEY')
SECRET_KEY=hashlib.sha256(env_key.encode()).digest()[:16]

class ETLEntity(ABC):
    def __init__(self, file_path: str, table_name: str, fields_map: Optional[list] = None, chunk_size: int = 100000):
        """
        Initialise the AbstractEntity class.
        Args:
            file_path: str - This defines the file path.
            fields_map: list - This defines the fields to be mapped.
            omop_table: str - This defines the table we are mapping to.
        """
        self._path = file_path
        self._fields_map = fields_map if fields_map else []
        self._chunk_size = chunk_size
        self._target_table = table_name
        # Initialize data as DataFrames
        self._source_data = pd.DataFrame(columns=self._fields_map)
        self._omop_data = pd.DataFrame(columns=self._fields_map)

    def load_data(self):
        """Load the source data from the file path."""
        try:
            chunks = pd.read_csv(self._path, chunksize=self._chunk_size, low_memory=False)
            # list to hold chunk while loading.
            df_list = []
            for chunk in tqdm(chunks, desc=f"Reading CSV for {self._target_table} in chunks..."):
                df_list.append(chunk)
            self._source_data = pd.concat(df_list, ignore_index=True).rename(columns=str.lower)
            logging.info(f"Data loaded successfully\n\n")
        
        except FileNotFoundError:
            logging.error(f"File not found: {self._path}")
        
        except pd.errors.ParserError:
            logging.error(f"Error parsing file: {self._path}")
        
        except Exception as e:
            logging.error(f"Unexpected error loading data: {e}")

    def set_fields(self, fields):
        """Set the fields."""
        self._fields_map = fields

    def get_fields(self):
        """Get the fields."""
        return self._fields_map

    def map_data_to_fields(self):
        """Extract only the required fields for OMOP mapping."""
        try:
            self._omop_data = self._source_data[self._fields_map]
            logging.info("Data mapped to specified fields successfully.")
        except KeyError as e:
            logging.error(f"Mapping failed. Missing fields: {e}")

    def get_omopped_data(self):
        """Get the OMOP mapped data."""
        return self._omop_data
    
    # Padding function to ensure fixed block size
    def pad_message(self, message, block_size=16):
        return message.ljust(block_size, ' ')  # Pad with spaces

    def remove_non_alphanumeric(self, value):
        import re
        return re.sub(r'[^a-zA-Z0-9]+', '', value)

    # Encrypt function
    def encrypt_value(self, raw_data):
        raw_data = raw_data.replace('-', '')
        message = self.pad_message(raw_data)  # Ensure it's 16 bytes
        cipher = AES.new(SECRET_KEY, AES.MODE_ECB)
        encrypted_bytes = cipher.encrypt(message.encode('utf-8'))
        
        # Convert to base64 and ensure exactly 30 characters
        encoded = base64.urlsafe_b64encode(encrypted_bytes).decode('utf-8')
        return encoded  # Trim to exactly 30 characters

    # Decrypt function
    def decrypt_value(self, encrypted_data):

        cipher = AES.new(SECRET_KEY, AES.MODE_ECB)
        
        # Fix base64 padding if needed
        missing_padding = 4 - (len(encrypted_data) % 4)
        encrypted_data += "=" * missing_padding  # Base64 padding fix
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_data)
        
        decrypted_message = cipher.decrypt(encrypted_bytes).decode('utf-8')
        return decrypted_message.strip()  # Remove extra spaces
    
    def unique_id_generator(self, source_id, source_type):
        """Generate a unique identifier.
        Args:
            source_id: str - This defines the source id from the table.
            source_type: str - This defines the source type mainly the table name.
        """

        # Using a deterministic UUID version 5 based on a namespace and the source_id
        namespace = uuid.NAMESPACE_DNS
        namespace = uuid.uuid5(namespace, source_type)
        return uuid.uuid5(namespace, source_id).int % (10**9)

    @abstractmethod
    def map_data(self, mapper: dict = {}):
        """Abstract method to map specific fields to OMOP format."""
        pass

    def run_mapping(self, fields):
        """Run the complete mapping process."""
        self.load_data()
        self.set_fields(fields=fields)
        self.map_data()
        self.map_data_to_fields()
