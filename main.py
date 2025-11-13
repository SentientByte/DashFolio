"""Entry point for running DashFolio calculations."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from importlib.util import find_spec
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from app_paths import CONFIG_FILE, DATA_STORE, PORTFOLIO_FILE
from services.configuration import ensure_default_config_file
from services.portfolio import ensure_default_portfolio_file


REQUIRED_RUNTIME_PACKAGES = (
    "pandas",
    "numpy",
    "yfinance",
)


def ensure_runtime_dependencies() -> None:
    """Ensure runtime dependencies are installed inside the project virtualenv."""

    missing = _detect_missing_packages(REQUIRED_RUNTIME_PACKAGES)
    if not missing:
        return

    project_root = Path(__file__).resolve().parent
    venv_dir = project_root / ".venv"

    if _running_inside_venv(venv_dir):
        _install_packages(Path(sys.executable), missing)
        missing_after_install = _detect_missing_packages(REQUIRED_RUNTIME_PACKAGES)
        if missing_after_install:
            missing_list = ", ".join(missing_after_install)
            raise SystemExit(
                "DashFolio could not install the required packages: "
                f"{missing_list}.\n"
                "Review the pip output above and resolve the issue manually."
            )
        return

    try:
        venv_python = _ensure_project_virtualenv(venv_dir)
    except subprocess.CalledProcessError as exc:
        raise SystemExit(
            "DashFolio could not create the project virtual environment automatically.\n"
            "Resolve the venv error above and run the application again."
        ) from exc

    _install_packages(venv_python, missing)
    print(
        "Restarting DashFolio using the project virtual environment after installing "
        "missing packages..."
    )
    os.execv(str(venv_python), [str(venv_python), *sys.argv])


def _detect_missing_packages(packages: Iterable[str]) -> list[str]:
    """Return a sorted list of packages that cannot be imported."""

    return sorted(package for package in packages if find_spec(package) is None)


def _ensure_project_virtualenv(venv_dir: Path) -> Path:
    """Create the project virtual environment if it does not yet exist."""

    if venv_dir.exists():
        return _venv_python_path(venv_dir)

    print(f"Creating project virtual environment at {venv_dir}...")
    subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True)
    return _venv_python_path(venv_dir)


def _running_inside_venv(venv_dir: Path) -> bool:
    """Return True when the current interpreter belongs to the project venv."""

    try:
        return Path(sys.prefix).resolve() == venv_dir.resolve()
    except FileNotFoundError:
        return False


def _venv_python_path(venv_dir: Path) -> Path:
    """Determine the Python executable inside the virtual environment."""

    if os.name == "nt":
        candidate = venv_dir / "Scripts" / "python.exe"
    else:
        candidate = venv_dir / "bin" / "python"

    if not candidate.exists():
        raise SystemExit(
            "DashFolio could not locate the Python executable in the project "
            "virtual environment.\n"
            "Try removing the '.venv' directory and run the application again."
        )

    return candidate


def _install_packages(python_executable: Path, packages: Iterable[str]) -> None:
    """Install packages using the given Python interpreter."""

    package_list = sorted(set(packages))
    if not package_list:
        return

    print(
        "Installing missing packages via pip: "
        + ", ".join(package_list)
    )
    try:
        subprocess.run(
            [str(python_executable), "-m", "pip", "install", *package_list],
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise SystemExit(
            "DashFolio was unable to install the required dependencies automatically.\n"
            "Resolve the pip error above and run the application again."
        ) from exc


ensure_runtime_dependencies()

from Calculations import (  # noqa: E402  (import after dependency validation)
    calculate_statistics,
    determine_start_date,
    load_config,
    load_portfolio,
    load_price_data,
    normalize_config,
    run_trailing_stop_analysis,
    update_portfolio_prices,
)


def main() -> None:
    ensure_default_config_file()
    ensure_default_portfolio_file()

    today = datetime.now(ZoneInfo("America/New_York"))

    raw_config = load_config(CONFIG_FILE)
    config = normalize_config(raw_config)

    start_date, period_reason = determine_start_date(
        config["DATA_PERIOD"],
        config["CUSTOM_START_DATE"],
        today,
    )
    start_date_str = start_date.strftime("%Y-%m-%d")

    print(
        f"DATA_PERIOD requested: '{config['DATA_PERIOD']}' -> "
        f"using start date {start_date_str} ({period_reason})"
    )

    df_portfolio = load_portfolio(PORTFOLIO_FILE, DATA_STORE)
    df_portfolio = update_portfolio_prices(df_portfolio, PORTFOLIO_FILE)
    tickers = df_portfolio["Ticker"].unique()

    all_data = load_price_data(tickers, start_date, today, DATA_STORE)

    calculate_statistics(
        all_data,
        tickers,
        config["SPAN_EWMA"],
        config["DATA_PERIOD"],
    )

    run_trailing_stop_analysis(
        df_portfolio,
        all_data,
        config["STOP_LOSS_PERCENTAGE_RANGE"],
        config["STOP_LOSS_STEP"],
        config["NUM_SIMULATIONS"],
        config["SPAN_EWMA"],
        config["CONFIDENCE_LEVEL"],
        config["DATA_PERIOD"],
        DATA_STORE,
    )


if __name__ == "__main__":
    main()
