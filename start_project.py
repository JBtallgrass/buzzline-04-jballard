import subprocess
import time
import pathlib

# Paths to the scripts
scripts = [
    "rafting_producer.py",
    "rafting_consumer.py",
    "csv_rafting_consumer.py",
    "csv_feedback_consumer.py",
    "jb_project_consumer.py"
]

# Directory where your scripts are located
scripts_dir = pathlib.Path(__file__).parent

def run_in_new_terminal(script_name):
    """Run a script in a new terminal window (cmd on Windows)."""
    script_path = scripts_dir / script_name
    if script_path.exists():
        print(f"🚀 Starting {script_name} in a new terminal...")
        subprocess.Popen(f'start cmd /k python {script_path}', shell=True)  # Opens a new cmd window
    else:
        print(f"❌ Script not found: {script_name}")

def main():
    try:
        for script in scripts:
            run_in_new_terminal(script)
            time.sleep(2)  # Delay between launches
        print("\n✅ All scripts launched in separate terminals.")
    except KeyboardInterrupt:
        print("⛔ Shutdown requested by user.")

if __name__ == "__main__":
    main()
