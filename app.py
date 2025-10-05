from flask import Flask, render_template, request, redirect, url_for
import json
import subprocess
import sys
import os
import pandas as pd

# ------------------------------
# Paths & constants
# ------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Prefer venv python if available, otherwise use current interpreter
VENV_PYTHON = os.path.join(BASE_DIR, 'venv', 'Scripts', 'python.exe')
if not os.path.exists(VENV_PYTHON):
    # fallback to current python interpreter (this ensures we run with an interpreter that has deps)
    VENV_PYTHON = sys.executable

MAIN_SCRIPT = os.path.join(BASE_DIR, 'main.py')
CONFIG_FILE = os.path.join(BASE_DIR, 'config.json')

app = Flask(__name__)
log_output_raw = []   # raw stdout lines from main.py
log_output_table = [] # parsed table (list of dicts) built from Excel result

# ------------------------------
# Ensure config file exists
# ------------------------------
if not os.path.exists(CONFIG_FILE):
    default_config = {
        "DATA_PERIOD": "1y",
        "CUSTOM_START_DATE": "2024-01-01",
        "STOP_LOSS_PERCENTAGE_RANGE": [1, 2],
        "STOP_LOSS_STEP": 0.2,
        "NUM_SIMULATIONS": 10000,
        "CONFIDENCE_LEVEL": 0.95,
        "SPAN_EWMA": 60
    }
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(default_config, f, indent=4)
    print(f"Created default config file: {CONFIG_FILE}")

# ------------------------------
# Helper functions
# ------------------------------
def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_config(config):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4)

def run_main_script():
    """
    Run main.py synchronously using the venv/python specified (VENV_PYTHON).
    Capture stdout/stderr into log_output_raw and after completion attempt to read the results Excel
    into log_output_table (list of dicts).
    """
    global log_output_raw, log_output_table
    log_output_raw = []
    log_output_table = []

    if not os.path.exists(MAIN_SCRIPT):
        log_output_raw.append(f"ERROR: main script not found at {MAIN_SCRIPT}")
        return

    # Execute main.py
    try:
        process = subprocess.Popen(
            [VENV_PYTHON, MAIN_SCRIPT],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=BASE_DIR
        )
    except Exception as e:
        log_output_raw.append(f"Failed to start process: {e}")
        return

    # Stream stdout lines
    for line in process.stdout:
        # store lines (strip trailing newlines for template)
        log_output_raw.append(line.rstrip('\n'))

    process.wait()

    # Once done, attempt to load the result Excel file (if created)
    try:
        config = load_config()
        data_period = config.get('DATA_PERIOD', '1y')
        results_file = os.path.join(BASE_DIR, f'trailing_stop_analysis_ewma_{data_period}.xlsx')
        if os.path.exists(results_file):
            df = pd.read_excel(results_file, engine='openpyxl')
            # convert column names to strings (safe for template)
            df.columns = [str(c) for c in df.columns]
            log_output_table = df.to_dict(orient='records')
        else:
            # no results file found; optionally include note
            log_output_raw.append(f"Note: results file not found: {results_file}")
    except Exception as e:
        log_output_raw.append(f"Error reading results file: {e}")

# ------------------------------
# Routes
# ------------------------------
@app.route('/', methods=['GET', 'POST'])
def index():
    config = load_config()
    if request.method == 'POST':
        # update config from form
        # Note: some basic input sanitization/typing
        config['DATA_PERIOD'] = request.form.get('DATA_PERIOD', config.get('DATA_PERIOD', '1y'))
        config['CUSTOM_START_DATE'] = request.form.get('CUSTOM_START_DATE', config.get('CUSTOM_START_DATE', '2024-01-01'))

        try:
            minp = float(request.form.get('STOP_LOSS_MIN', config.get('STOP_LOSS_PERCENTAGE_RANGE', [1,2])[0]))
            maxp = float(request.form.get('STOP_LOSS_MAX', config.get('STOP_LOSS_PERCENTAGE_RANGE', [1,2])[1]))
            config['STOP_LOSS_PERCENTAGE_RANGE'] = [minp, maxp]
        except Exception:
            # keep old values on error
            pass

        try:
            config['STOP_LOSS_STEP'] = float(request.form.get('STOP_LOSS_STEP', config.get('STOP_LOSS_STEP', 0.2)))
        except Exception:
            pass

        try:
            config['NUM_SIMULATIONS'] = int(request.form.get('NUM_SIMULATIONS', config.get('NUM_SIMULATIONS', 10000)))
        except Exception:
            pass

        try:
            config['CONFIDENCE_LEVEL'] = float(request.form.get('CONFIDENCE_LEVEL', config.get('CONFIDENCE_LEVEL', 0.95)))
        except Exception:
            pass

        try:
            config['SPAN_EWMA'] = int(request.form.get('SPAN_EWMA', config.get('SPAN_EWMA', 60)))
        except Exception:
            pass

        save_config(config)
        return redirect(url_for('index'))

    # GET
    return render_template('index.html',
                           config=config,
                           log_output_raw=log_output_raw,
                           log_output_table=log_output_table)

@app.route('/run', methods=['POST'])
def run():
    # Make sure any previous logs are cleared and show starting message immediately
    global log_output_raw
    log_output_raw = ["Starting calculations..."]
    # Run synchronously (Option 1). Will block until main.py completes.
    run_main_script()
    return redirect(url_for('index'))

# ------------------------------
# Run app
# ------------------------------
if __name__ == '__main__':
    app.run(debug=True)
