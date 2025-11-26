import os
import subprocess
import sys
from pathlib import Path
import yaml

# --- Pre-requisites ---
# This script needs python-dotenv to load the .env file.
# We will install it automatically.

def install_prerequisites():
    """Installs the libraries needed to run this setup script."""
    print("Checking for setup script prerequisites (python-dotenv, PyYAML)...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "python-dotenv", "pyyaml"])
    except subprocess.CalledProcessError:
        print("ERROR: Failed to install prerequisite libraries. Please install 'python-dotenv' and 'pyyaml' manually using pip.")
        sys.exit(1)

# Must be called after prerequisites are installed
install_prerequisites()
from dotenv import load_dotenv

# --- Configuration ---
DBT_PROJECT_NAME = "my_dbt_project"  # Should match the 'name' in your dbt_project.yml
REQUIRED_DBT_PACKAGES = ["dbt-snowflake"] # The dbt adapter to install

def load_environment_variables():
    """Loads environment variables from a .env file in the current directory."""
    env_path = Path('.') / '.env'
    if not env_path.exists():
        print("ERROR: .env file not found!")
        print("Please create a .env file with your Snowflake credentials (see documentation).")
        sys.exit(1)
    
    print("Loading environment variables from .env file...")
    load_dotenv()
    print("Environment variables loaded.")

def install_dbt_packages():
    """Installs the required dbt adapter packages."""
    print("Checking and installing required dbt packages...")
    for package in REQUIRED_DBT_PACKAGES:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"Successfully installed or verified {package}")
        except subprocess.CalledProcessError:
            print(f"ERROR: Failed to install {package}. Please check your pip configuration.")
            sys.exit(1)

def get_dbt_profiles_path():
    return Path('.') / f"{DBT_PROJECT_NAME}/profiles.yml"

def create_or_update_profile(profiles_path):
    """
    Creates or updates the profiles.yml file with the Snowflake configuration
    sourced from environment variables.
    """
    print(f"Checking dbt profile at: {profiles_path}")
    profiles_path.parent.mkdir(exist_ok=True) # Ensure the .dbt directory exists

    # --- Define the Snowflake profile using Environment Variables ---
    new_profile_config = {
        DBT_PROJECT_NAME: {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "snowflake",
                    "account": os.getenv("DBT_SNOWFLAKE_ACCOUNT"),
                    "user": os.getenv("DBT_USER"),
                    "password": os.getenv("DBT_PASS"),
                    "role": os.getenv("DBT_ROLE"),
                    "database": os.getenv("DBT_DBNAME"),
                    "warehouse": os.getenv("DBT_WAREHOUSE"),
                    "schema": os.getenv("DBT_SCHEMA"),
                    "threads": 4, # A sensible default
                    "client_session_keep_alive": False,
                }
            }
        }
    }

    # --- Check for missing environment variables ---
    required_env_vars = [
        "DBT_SNOWFLAKE_ACCOUNT", "DBT_USER", "DBT_PASS", "DBT_ROLE", 
        "DBT_WAREHOUSE", "DBT_DBNAME", "DBT_SCHEMA"
    ]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        print("\nERROR: Missing required environment variables!")
        print("Please ensure the following variables are set in your .env file:")
        for var in missing_vars:
            print(f"- {var}")
        sys.exit(1)

    # --- Read existing profiles.yml or create new config dictionary ---
    if profiles_path.exists():
        with open(profiles_path, 'r') as f:
            try:
                config = yaml.safe_load(f) or {}
            except yaml.YAMLError as e:
                print(f"ERROR: Could not parse existing profiles.yml. Please check for syntax errors. {e}")
                sys.exit(1)
    else:
        print("profiles.yml not found. Creating a new one.")
        config = {}

    # --- Append our project profile only if it doesn't exist ---
    if DBT_PROJECT_NAME in config:
        print(f"Profile '{DBT_PROJECT_NAME}' already exists in profiles.yml. Skipping update.")
    else:
        print(f"Profile '{DBT_PROJECT_NAME}' not found. Appending it to profiles.yml.")
        config.update(new_profile_config)
        
        with open(profiles_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        print("Successfully updated profiles.yml.")

if __name__ == "__main__":
    print("--- Starting dbt Project Setup for Snowflake ---")
    load_environment_variables()
    install_dbt_packages()
    profiles_yml_path = get_dbt_profiles_path()
    create_or_update_profile(profiles_yml_path)
    print("\n--- Setup Complete! ---")
    print("Verify your connection by running: dbt debug")
